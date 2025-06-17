export REPO="sfpscogs-ps-jb-azuseat2.registry.snowflakecomputing.com/openai/snowguide/snowguide_repository"
export SERVICE="snowguide_webcrawler"

docker build --rm --platform linux/amd64 -t ${REPO}/${SERVICE} -f ./Dockerfile . && \
docker push ${REPO}/${SERVICE}:latest && \
docker images | grep "snowguide_webcrawler"

#echo "Running..."
#snowsql -c azuseast2 -d OPENAI -s SNOWGUIDE -w SNOWGUIDE_WH -r SNOWGUIDE_ROLE -f ../sql/run_webcrawler_spcs.sql
#echo "Completed."