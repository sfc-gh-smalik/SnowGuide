# Running in Snowpark Container Services

This component will process new or changed url's discovered by the Crawler component. It retrieves the text of the page, cleans it, chunks it, and stores it in the CRAWLER_DISCOVERED_URLS table in Snowflake. A cortex search service is built on this table in order to use it for RAG generation.

**Usage**
```
cd loader
python loadcontent.py
```
## Error Handling
This process may be restarted or rerun at any time.

## License
This code is provided as - is. Use at your own risk.

**Contributing**
  - Feel free to submit issues and enhancement requests.