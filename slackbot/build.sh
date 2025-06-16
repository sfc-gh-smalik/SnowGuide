export REPO="sfpscogs-ps-jb-azuseat2.registry.snowflakecomputing.com/openai/snowguide/snowguide_repository"
export SERVICE="snowguide"
docker build --rm --platform linux/amd64 -t ${REPO}/${SERVICE} . && \
docker push ${REPO}/${SERVICE}:latest

snowsql -c azuseast2 -f ../sql/create_app.sql
