# To get this full repo url run SHOW IMAGE REPOSITORIES in schema snowguide.snowguide; and copy "repository_url" in Snowflake.
export REPO="<org name>-<account name>.registry.snowflakecomputing.com/snowguide/snowguide/snowguide_repository"
export SERVICE="snowguide"
docker build --rm --platform linux/amd64 -t ${REPO}/${SERVICE} . && \
docker push ${REPO}/${SERVICE}:latest

snowsql -f ../sql/create_slackbot_service.sql
