FROM python:3.9-slim-buster

ARG CODEARTIFACT_PIPY_URL
ARG GAINY_COMPUTE_VERSION
RUN pip install -U --index-url ${CODEARTIFACT_PIPY_URL} gainy-compute==${GAINY_COMPUTE_VERSION}

WORKDIR /srv
COPY . .
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "-u"]