import argparse
from typing import Iterable

from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.docstore.document import Document
from langchain_community.document_loaders import DirectoryLoader, TextLoader
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import Qdrant
from sec_edgar_downloader import Downloader
from xbrl import XBRLParser, XBRLParserException

from store import close_store_conn, init_store, get_info, save_info
from structs import TickerType, DocType

import logging


log = logging.getLogger('MAIN')

_COMPANY = 'Company'
_EMAIL = 'my.email@domain.com'

# envs
OPENAI_API_KEY = ''
QDRANT_URL = ''
QDRANT_API_KEY = None


def download_documents(ticker: TickerType,
                       doc_type: DocType,
                       save_dir: str,
                       docs_number: int | None = None) -> str:
    """
    Downloads SEC filings for a given ticker and saves them to a given directory.
    Loads new documents only if they do not exist.

    Args:
        ticker (str): The ticker symbol of the company.
        doc_type (Literal['10-K', '10-Q', '8-K']): The type of SEC document to download.
        save_dir (str): The directory where the downloaded documents will be saved.
                        If the directory does not exist, it will be created.
                        Full path to docs is 'save_dir/ticker/doc_type/'
        docs_number (int | None): The number of documents to download.

    Returns:
        Directory path to the downloaded documents.

    Raises:
        None

    Examples:
        download_documents('AAPL', '10-K', '/path/to/save')
    """

    ticker = ticker.upper()
    dl = Downloader(_COMPANY, _EMAIL, save_dir)
    try:
        dl.get(doc_type, ticker, limit=docs_number)
    except Exception as e:
        raise Exception(
            f"Failed to fetch document for {ticker} from Edgar database"
        ) from e
    return f'{save_dir}/sec-edgar-filings/{ticker}/{doc_type}/'


def combine_documents(dir_paths: Iterable[str]) -> Document:
    """
    Combine multiple documents into a single Document object.

    Args:
        dir_paths (Iterable[str]): An iterable of directory paths containing the documents to be combined.

    Returns:
        Document: A Document object that represents the combined content of all the documents.

    Raises:
        XBRLParserException: If a document is not a valid XBRL document.
        IndexError: If a document is not a valid XBRL document.
        Exception: If there is a general parsing failure.
    """
    xbrl_parser = XBRLParser()
    xbrl_texts = []
    for dir_path in dir_paths:
        dir_loader = DirectoryLoader(
            dir_path, glob="**/*.txt", loader_cls=TextLoader
        )
        documents = dir_loader.load()
        for document in documents:
            try:
                xbrl_doc = xbrl_parser.parse(document.metadata['source'])
            except XBRLParserException as e:
                log.warning(f'Skipped document [{document.metadata["source"]}]: is not a XBRL document')
                continue
            except IndexError as e:
                log.warning(f'Skipped document [{document.metadata["source"]}]: is not a valid XBRL document. {e}')
                continue
            except Exception as e:
                log.exception(f'Failed to parse document [{document.metadata["source"]}]')
                continue
            p_span_tags = xbrl_doc.find_all(lambda x: (x.name == 'p' or 'div') and x.find('span'))
            xbrl_text = ' '.join(tag.get_text() for tag in p_span_tags)
            xbrl_texts.append(xbrl_text)

    all_document_texts = '\n\n'.join(xbrl_texts)
    return Document(page_content=all_document_texts, metadata={})


def split_documents(docs: Iterable[Document]) -> list[Document]:
    """
    Split a list of documents into smaller chunks using a RecursiveCharacterTextSplitter.

    Args:
        docs (Iterable[Document]): An iterable of Document objects to be split.

    Returns:
        list[Document]: A list of Document objects representing the split chunks.
    """
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1200, chunk_overlap=20
    )
    return text_splitter.split_documents(docs)


def vectorize_and_save_to_store(text_chunks: list[Document], collection_name: str) -> Qdrant:
    """
    Vectorize a list of text chunks using OpenAIEmbeddings and save them to a Qdrant collection.

    Args:
        text_chunks (list[Document]): A list of Document objects representing the text chunks to be vectorized and saved.
        collection_name (str): The name of the Qdrant collection to save the vectorized chunks to.

    Returns:
        Qdrant: A Qdrant object representing the saved collection.
    """
    embedding = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
    return Qdrant.from_documents(
        text_chunks,
        embedding,
        url=QDRANT_URL,
        api_key=QDRANT_API_KEY,
        prefer_grpc=True,
        collection_name=collection_name,
    )


def pipeline(ticker: TickerType, doc_type: DocType, save_dir: str) -> None:
    """
    Execute a pipeline of operations to process and store documents.

    Args:
        ticker (TickerType): The ticker symbol representing the company.
        doc_type (DocType): The type of documents to process.
        save_dir (str): The directory path to save the downloaded documents.

    Returns:
        None

    Examples:
        >>> ticker = "AAPL"
        >>> doc_type = "10-K"
        >>> save_dir = "."
        >>> pipeline(ticker, doc_type, save_dir)
    """
    actually_docs_save_path = download_documents(ticker, doc_type, save_dir)
    log.info(f'Downloaded {doc_type} documents for {ticker}')

    save_info(ticker, doc_type, actually_docs_save_path)
    log.info(f'Saved {doc_type} documents for {ticker}')

    docs_path = get_info(ticker, doc_type)[0][3]
    combined_docs = combine_documents(dir_paths=[docs_path])
    log.info(f'Combined {doc_type} documents for {ticker}')

    splitted_docs = split_documents(docs=[combined_docs])
    log.info(f'Split texts for {ticker} {doc_type} into {len(splitted_docs)} chunks')

    collection_name = f'{ticker}_{doc_type}'.lower()
    log.info(f'Embedding and Saving {doc_type} documents for {ticker} to Qdrant...')
    qdrant_store = vectorize_and_save_to_store(text_chunks=splitted_docs, collection_name=collection_name)
    log.info(f'Uploaded {ticker} {doc_type} to Qdrant to {collection_name} collection')


def main(args: argparse.Namespace) -> None:
    ticker = args.ticker.upper()
    doc_type = args.doctype.upper()
    save_dir = args.save_dir

    init_store()

    try:
        pipeline(ticker, doc_type, save_dir)
    except KeyboardInterrupt:
        exit(0)
    except Exception as e:
        log.exception(f'Problem executing the pipeline for {ticker} {doc_type}')
    finally:
        close_store_conn()
        log.info('Exit')
