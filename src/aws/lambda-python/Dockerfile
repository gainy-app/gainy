ARG BASE_IMAGE_VERSION
ARG BASE_IMAGE_REGISTRY_ADDRESS
FROM ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-lambda-python:${BASE_IMAGE_VERSION}

ARG CODEARTIFACT_PIPY_URL
ARG GAINY_COMPUTE_VERSION
RUN pip install -U --index-url ${CODEARTIFACT_PIPY_URL} gainy-compute==${GAINY_COMPUTE_VERSION}

COPY requirements.txt /requirements.txt
RUN pip install --requirement /requirements.txt

COPY lambda_python/ ${LAMBDA_TASK_ROOT}
RUN chmod -R a+rX ${LAMBDA_TASK_ROOT}