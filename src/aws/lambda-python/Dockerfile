FROM public.ecr.aws/lambda/python:3.9

RUN pip install poetry

COPY poetry.lock pyproject.toml ./
RUN poetry export | python3 -mpip install -r /dev/stdin

COPY lambda_python/ ${LAMBDA_TASK_ROOT}
RUN chmod -R a+rX ${LAMBDA_TASK_ROOT}