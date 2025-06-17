ARG BASE_IMAGE=python:3.11-slim-buster
FROM $BASE_IMAGE
COPY requirements.txt ./
COPY crawl_w_local.py ./
COPY spcs_utils.py ./
COPY .env ./
RUN pip install -r ./requirements.txt
ENTRYPOINT ["python3", "crawl_w_local.py"]
