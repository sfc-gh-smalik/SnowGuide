ARG BASE_IMAGE=python:3.11-slim-buster
FROM $BASE_IMAGE
COPY requirements.txt ./
COPY loadcontent.py ./
COPY spcs_utils.py ./
COPY .env ./
RUN pip install -r ./requirements.txt
ENTRYPOINT ["python3", "loadcontent.py"]