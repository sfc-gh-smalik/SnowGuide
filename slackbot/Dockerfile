ARG BASE_IMAGE=python:3.10-slim-buster
FROM $BASE_IMAGE
COPY app.py ./
COPY spcs_utils.py ./
COPY .env ./
RUN pip install --upgrade pip && \
    pip install flask pycurl requests slack_bolt snowflake-connector-python dotenv && \
    pip install snowflake-snowpark-python snowflake.core openai
CMD ["python3", "app.py"]
