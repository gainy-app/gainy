ARG BASE_IMAGE_VERSION
ARG BASE_IMAGE_REGISTRY_ADDRESS
ARG CODEARTIFACT_PIPY_URL

FROM ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-lambda-python:${BASE_IMAGE_VERSION}

COPY requirements.txt /requirements.txt
RUN pip install --requirement /requirements.txt --index-url ${CODEARTIFACT_PIPY_URL}

COPY lambda_python/ ${LAMBDA_TASK_ROOT}
RUN chmod -R a+rX ${LAMBDA_TASK_ROOT}