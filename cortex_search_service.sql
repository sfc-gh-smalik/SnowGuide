

-- Create a Cortex Search Service once the URL are scraped and content is pulled successfully
CREATE OR REPLACE CORTEX SEARCH SERVICE <fully_qualified_service_name>
  ON chunked_data
  WAREHOUSE = <wh_name>
  TARGET_LAG = '<lag>'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
  INITIALIZE = ON_CREATE
  AS 
    SELECT u.*, index, value::string as chunked_data FROM <your_fully_qualified_content_table> u,
    LATERAL FLATTEN(input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER (cleaned_text,'markdown',2000,300));


-- Cortex Search Preview query
SELECT
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW (
      '<fully_qualified_service_name>',
      '{
          "query": "what connectors are available in openflow",
          "columns": ["CHUNKED_DATA", "URL"],
          "limit": 20
      }'
  );
