-- Create a Cortex Search Service once the URL are scraped and content is pulled successfully
CREATE CORTEX SEARCH SERVICE IF NOT EXISTS snowguide.snowguide.doc_searcher
  ON chunked_data
  WAREHOUSE = snowguide_wh
  TARGET_LAG = '7 days'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
  INITIALIZE = ON_SCHEDULE
  AS 
    SELECT u.*, index, value::string as chunked_data FROM snowguide.snowguide.crawler_discovered_urls u,
    LATERAL FLATTEN(input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER (cleaned_text,'markdown',2000,300));

ALTER CORTEX SEARCH SERVICE snowguide.snowguide.doc_searcher REFRESH;
ALTER CORTEX SEARCH SERVICE snowguide.snowguide.doc_searcher RESUME;

-- Cortex Search Preview query
SELECT
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW (
      'SNOWGUIDE.SNOWGUIDE.DOC_SEARCHER',
      '{
          "query": "what connectors are available in openflow",
          "columns": ["CHUNKED_DATA", "URL"],
          "limit": 20
      }'
  );
x