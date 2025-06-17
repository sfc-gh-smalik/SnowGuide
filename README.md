# SnowGuide

A Snowflake Docs and Knowledge Chatbot/Slackbot. This application crawls the documenation in docs.snowflake.com, extracts it, and stores it in Snowflake tables. A Chatbot service then used Snowflake Cortex search to retrieve the most relevant docs for a question and uses them to enhance the prompt send to an LLM. This is currently setup to use either OpenAI's API or the Cortex "complete" API with a model served by Snowflake.

This application consists of three major components:

| Component | Description |
| ---- | ----|
|[Web Crawler](./docs/crawler.md) |Crawls web pages and extracts text. Can do full or incremental refresh. Document url's and hashes are then uploaded to Snowflake tables. This service should be scheduled to run periodically to pickup new or changed pages.|
|[Web Content Loader](./docs/loader.md) |Read new or updated links created by the Crawler, extracts the text, chunks it, and loads into Snowflake tables. This service should be run after each run of the Crawler.|
|[Slackbot Service](./docs/slackbot.md)|This is a long-running service that registers itself with Slack and is called to respond to prompts within a channel.|

# Running the Project
The components in this project can be run several ways
1. Locally (for development or debugging)
2. Snowflake Notebook (except for Slackbot) See [Running in Notebooks](.docs/notebooks.md)
3. Snowflake SPCS Containers See [Running in SPCS](.docs/spcs.md)

Note: After the first run of the Crawler you need to create the Cortex search service before running the Slackbot. This is found in [sql/cortex_search_service.sql](./sql/cortex_search_service.sql).

# Common Setup

## Snowflake
This project assumes that it will have it's own schema within Snowflake as well as several account-level objects (API integrations, warehouses, etc.) See the script in  [sql/setup.sql](./sql/setup.sql) for details. Make changes for your environment and run this script once.

## Environment File
There is a sample_env file in the root of this repository. You need to update this with 
your values and rename it to ".env". Currently you need to copy it to each of the sub-folders.
```
cp .env ./crawler
cp .env ./loader
cp .env ./slackbot
```

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
|LLM_API||cortex &#124; openai|
|OPENAI_API_KEY||Only required when LLM_API=openai|

## License
This code is provided as - is. Use at your own risk.

**Contributing**

Feel free to submit issues and enhancement requests.

