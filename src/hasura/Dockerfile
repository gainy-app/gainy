ARG BASE_IMAGE_VERSION
ARG BASE_IMAGE_REGISTRY_ADDRESS

FROM ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-hasura:${BASE_IMAGE_VERSION} as production

COPY hasura /hasura
COPY hasura_tests /hasura_tests

FROM production as test

RUN apt update && apt install -y libpq-dev
RUN pip3 install psycopg2-binary==2.9.5
