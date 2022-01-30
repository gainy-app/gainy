ARG BASE_IMAGE_VERSION
ARG BASE_IMAGE_REGISTRY_ADDRESS
ARG CODEARTIFACT_PIPY_URL

FROM ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-meltano:${BASE_IMAGE_VERSION}

COPY meltano/data /project/data
COPY meltano/orchestrate /project/orchestrate
COPY meltano/scripts /project/scripts
COPY meltano/seed /project/seed
COPY meltano/transform /project/transform
COPY meltano/configs /project/configs
COPY meltano/meltano.template.yml meltano/symbols* meltano/exchanges* /project/

ENV ENV=local
ENV EODHISTORICALDATA_JOBS_COUNT=1

COPY ./meltano/requirements.txt /requirements.txt
RUN pip install --requirement /requirements.txt --index-url ${CODEARTIFACT_PIPY_URL}
