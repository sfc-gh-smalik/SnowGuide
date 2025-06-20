#export REPO="sfpscogs-ps-jb-azuseat2.registry.snowflakecomputing.com/openai/snowguide/snowguide_repository"
export REPO="sfpscogs-jbarnett.registry.snowflakecomputing.com/snowguide/snowguide/snowguide_repository"
export SERVICE="snowguide"
docker build --rm --platform linux/amd64 -t ${REPO}/${SERVICE} . && \
docker push ${REPO}/${SERVICE}:latest

snowsql -f ../sql/create_slackbot_service.sql
