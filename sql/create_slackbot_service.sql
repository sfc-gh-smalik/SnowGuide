-----------------------------------------------------------------------------------------------
-- Create Snowguide Slackbot SPCS Service
------------------------------------------------------------------------------------------------
use role snowguide_role;
use warehouse snowguide_wh;
use schema snowguide.snowguide;

CREATE SERVICE IF NOT EXISTS snowguide_service
IN COMPUTE POOL SNOWGUIDE_COMPUTE_POOL
EXTERNAL_ACCESS_INTEGRATIONS = (slack_apis_access_integration)
  FROM SPECIFICATION $$
    spec:
      containers:
      - name: main
        image: /snowguide/snowguide/snowguide_repository/snowguide:latest
        env:
          SERVER_PORT: 8000
      endpoints:
      - name: snowguideendpoint
        port: 8000
        public: true
      $$
;

-- Use the following to refresh after uploading a new image
ALTER SERVICE snowguide_service suspend;
ALTER SERVICE snowguide_service
  FROM SPECIFICATION $$
    spec:
      containers:
      - name: main
        image: /openai/snowguide/snowguide_repository/snowguide:latest
        env:
          SERVER_PORT: 8000
      endpoints:
      - name: snowguideendpoint
        port: 8000
        public: true
      $$;
ALTER SERVICE snowguide_service resume;

------------------------------------------------------------------------------------------------
-- Wait until it is up
------------------------------------------------------------------------------------------------
EXECUTE IMMEDIATE $$
DECLARE 
    status varchar := 'PENDING';
BEGIN
    WHILE (status = 'PENDING') DO
        SHOW SERVICE CONTAINERS IN SERVICE snowguide_service;
        SELECT "status" INTO :status FROM table(result_scan(last_query_id()));
        EXECUTE IMMEDIATE 'SELECT SYSTEM$WAIT(1)';
    END WHILE;
    
    RETURN status;
END;
$$;

SHOW ENDPOINTS IN SERVICE snowguide_service ->> SELECT 'ENDPOINTS' as description, * FROM $1;
SHOW SERVICE CONTAINERS IN SERVICE snowguide_service ->> SELECT 'CONTAINERS' as description, * FROM $1;

-- This query will print logs from the container
/* 
SELECT index, value AS log_line
  FROM TABLE(
   SPLIT_TO_TABLE(SYSTEM$GET_SERVICE_LOGS('snowguide_service', '0', 'main'), '\n')
  )
  WHERE TRIM(value) <> ''
  ORDER BY index desc limit 300;
*/