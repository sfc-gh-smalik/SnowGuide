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
|SNOWFLAKE_USER|YES||
|SNOWFLAKE_USER_ROLE|YES||
|SNOWFLAKE_ACCOUNT|YES|\<ORG-ACCOUNT>|
|HOST||\<ORG-ACCOUNT>.snowflakecomputing.com|
|SNOWFLAKE_PASSWORD||If required then use a Programmatic Access Token|
||
|**SLACK BOT SPECIFIC**|
|SLACK_BOT_TOKEN|YES||
|SLACK_SIGNING_SECRET|YES||
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


# SnowGuide Setup
## Build and Deploy
To build and deploy this service run [build.sh](build.sh). You will need to make a few changes first such as the account name for the image repository. You will also need to authenticate manually to that repo using
```
docker login <repo url>
```
This process builds an image file with our application and uploads it to an image repository in snowflake. It then will create or alter the service that uses this. It uses snowsql and assumes that the target connection is your DEFAULT!

## License
This code is provided as - is. Use at your own risk.

**Contributing**

Feel free to submit issues and enhancement requests.

