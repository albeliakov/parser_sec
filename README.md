### Start
```shell
python run.py TICKER DOC_TYPE --save-dir SAVE_DIR
```
TICKER_NAME: ticker (e.g. AAPL)  
DOC_TYPE: document type ('10-K', '10-Q', '8-K')  
SAVE_DIR: optional argument, directory to save the documents. By default, it is current dir 

### Pipeline
Getting xblr documents from sec.gov -> Saving them -> Combining documents into one document -> Splitting document -> Embedding -> Saving to Qdrant