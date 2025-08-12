# SnowGuide

A Snowflake Docs and Knowledge Chatbot/Slackbot. This application uses Snowflake documenation (docs.snowflake.com) via Cortex Knowledge Extensions. A Chatbot service then uses Snowflake Cortex search to retrieve the most relevant docs for a question and uses them to enhance the prompt send to an LLM. This is currently setup to use either OpenAI's API or the Cortex "complete" API with a model served by Snowflake.

# SnowGuide

A Snowflake Docs and Knowledge Slackbot

**Usage**
This is configured to run in a Snowflake SPCS container and connect directly to slack.

# Common Setup

## Snowflake
This project assumes that it will have it's own schema within Snowflake as well as several account-level objects (API integrations, warehouses, etc.) See the script in  [..sql/setup.sql](../sql/setup.sql) for details. Make changes for your environment and run this script once.

## Environment File
There is a sample_env file in the root of this repository. You need to update this with 
your values and rename it to ".env". Currently you need to copy it to each of the sub-folders.
```
cp .env ./slackbot
```

## Environment file (Do Before Build and Deploy)
The following describes the variables set in the environment file.

| Variable | Required| Description|
| -------- | ------- | ------- |
|**COMMON**|
|SNOWFLAKE_DATABASE|YES||
|SNOWFLAKE_SCHEMA|YES||
|SNOWFLAKE_WAREHOUSE|YES||
|SNOWFLAKE_USER||Only required to run locally.|
|SNOWFLAKE_USER_ROLE|YES||
|SNOWFLAKE_ACCOUNT|YES|\<ORG-ACCOUNT>|
|HOST||\<ORG-ACCOUNT>.snowflakecomputing.com|
|SNOWFLAKE_PASSWORD||Only required to run locally. If required then use a Programmatic Access Token|
||
|**SLACK BOT SPECIFIC**|
|SLACK_BOT_TOKEN|YES||
|SLACK_APP_TOKEN|YES||
|SLACK_USER_TOKEN|YES||
||
|**Cortex search and LLM**|
|LLM_API||cortex &#124; openai|
|OPENAI_API_KEY||(Optional. Required if LLM_API=openai)|
|LLM_API||cortex or openai|
|CORTEX_SEARCH_DATABASE||'SNOWFLAKE_DOCUMENTATION'|
|CORTEX_SEARCH_SCHEMA||'CKE_SNOWFLAKE_DOCS_SERVICE'|
|CORTEX_SEARCH_SERVICE||'CKE_SNOWFLAKE_DOCS_SERVICE'|
|CORTEX_SEARCH_COLUMN||'chunk'|
|CORTEX_URL_COLUMN||'source_url'|

# Slack App setup
You will need to create a Slack application before deploying this solution. There are several key or token values that will be needed. ["docs/Setting up Slack App.docx"](Setting up Slack App) for details.

# SnowGuide Setup
## Build and Deploy
To build and deploy this service run [build.sh](build.sh). You will need to make a few changes first such as the account name for the image repository. To get this repo url run the following in Snowflake

1. Get the repo url by running the following in Snowflake:
```
SHOW IMAGE REPOSITORIES in schema snowguide.snowguide --> SELECT "repository_url" FROM $1;
```

2. Create a connection entry in ~/.snowsql/config. [https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#add-a-connection](docs).

3. Edit the build.sh and replace the repository name and the connection name

4. Manually authenticate to the repository by running the following:
You will also need to authenticate manually to that repo using
```
docker login <repo url>
```
5. Run the build script, which will deploy to the Snowflake image repository, build the service, and start it.
```
./build.sh
```
6. Check the status of the service using the following sql:
```
SHOW SERVICE CONTAINERS IN SERVICE snowguide_service;
```

7. You can view the logs from the running container using the following sql;
```
-- This query will print logs from the container
SELECT value AS log_line
  FROM TABLE(
   SPLIT_TO_TABLE(SYSTEM$GET_SERVICE_LOGS('snowguide_service', '0', 'main'), '\n')
  )
  WHERE TRIM(value) <> ''
  --order by index;
  ORDER BY index desc limit 300;
```

This process builds an image file with our application and uploads it to an image repository in snowflake. It then will create or alter the service that uses this. It uses snowsql and assumes that the target connection is your DEFAULT!

## License
This code is provided as - is. Use at your own risk.

**Contributing**

Feel free to submit issues and enhancement requests.

