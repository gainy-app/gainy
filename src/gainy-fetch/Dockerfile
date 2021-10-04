ARG BASE_IMAGE_VERSION
ARG BASE_IMAGE_PREFIX

FROM ${BASE_IMAGE_PREFIX}-meltano:${BASE_IMAGE_VERSION}

COPY meltano/data /project/data
COPY meltano/orchestrate /project/orchestrate
COPY meltano/scripts /project/scripts
COPY meltano/seed /project/seed
COPY meltano/transform /project/transform
COPY meltano/meltano.template.yml meltano/symbols* /project/

ENV ENV=local
ENV EODHISTORICALDATA_JOBS_COUNT=1
