ARG BASE_IMAGE_VERSION
ARG BASE_IMAGE_REGISTRY_ADDRESS

FROM ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-meltano:${BASE_IMAGE_VERSION}

ARG CODEARTIFACT_PIPY_URL
ARG GAINY_COMPUTE_VERSION
RUN pip install -U --index-url ${CODEARTIFACT_PIPY_URL} gainy-compute==$GAINY_COMPUTE_VERSION

COPY meltano/data /project/data
COPY meltano/orchestrate /project/orchestrate
COPY meltano/scripts /project/scripts
COPY meltano/seed /project/seed
COPY meltano/transform /project/transform
COPY meltano/catalog /project/catalog
COPY meltano/meltano.template.yml meltano/symbols* meltano/exchanges* /project/

ENV ENV=local
ENV EODHISTORICALDATA_JOBS_COUNT=1
