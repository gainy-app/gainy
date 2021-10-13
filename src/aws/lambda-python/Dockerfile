ARG BASE_IMAGE_VERSION
ARG BASE_IMAGE_REGISTRY_ADDRESS

FROM ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-lambda-python:${BASE_IMAGE_VERSION}

COPY poetry.lock pyproject.toml /var/
RUN cd /var && poetry export | python3 -mpip install -r /dev/stdin

COPY lambda_python/ ${LAMBDA_TASK_ROOT}
RUN chmod -R a+rX ${LAMBDA_TASK_ROOT}