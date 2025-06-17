export REPO="sfpscogs-ps-jb-azuseat2.registry.snowflakecomputing.com/openai/snowguide/snowguide_repository"
export SERVICE="snowguide_content_loader"
docker build --rm --platform linux/amd64 -t ${REPO}/${SERVICE} . && \
docker push ${REPO}/${SERVICE}:latest

#echo "Running..."
#snowsql -c azuseast2 -d OPENAI -s SNOWGUIDE -w SNOWGUIDE_WH -r SNOWGUIDE_ROLE -f ../sql/run_contentloader_spcs.sql
#echo "Completed."