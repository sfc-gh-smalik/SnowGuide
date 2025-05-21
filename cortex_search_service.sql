

-- Create a Cortex Search Service once the URL are scraped and content is pulled successfully
CREATE OR REPLACE CORTEX SEARCH SERVICE <fully_qualified_service_name>
  ON content
  WAREHOUSE = <wh_name>
  TARGET_LAG = '<lag>'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
  INITIALIZE = ON_CREATE
  AS 
    SELECT * FROM <your_fully_qualified_content_table>;



-- Cortex Search Preview query
SELECT
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW (
      'TEMP.SMALIK.DOC_SEARCHER',
      '{
          "query": "what connectors are available in openflow",
          "columns": ["CONTENT", "PARENT_URL"],
          "limit": 20
      }'
  );
