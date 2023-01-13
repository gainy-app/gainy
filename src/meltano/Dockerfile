ARG BASE_IMAGE_VERSION
ARG BASE_IMAGE_REGISTRY_ADDRESS
FROM ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-meltano:${BASE_IMAGE_VERSION}

ARG CODEARTIFACT_PIPY_URL
ARG GAINY_COMPUTE_VERSION
RUN pip install -U --index-url ${CODEARTIFACT_PIPY_URL} gainy-compute==${GAINY_COMPUTE_VERSION}
RUN pip install -U algoliasearch==2.6.0

COPY meltano/data /project/data
COPY meltano/orchestrate /project/orchestrate
COPY meltano/scripts /project/scripts
COPY meltano/seed /project/seed
COPY meltano/transform /project/transform
COPY meltano/catalog /project/catalog
COPY meltano/meltano.template.yml meltano/symbols* meltano/exchanges* /project/
COPY init.d /init.d
COPY start-scheduler.sh /
RUN chmod +x /start-scheduler.sh

ENV ENV=local
