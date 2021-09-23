FROM public.ecr.aws/lambda/nodejs:12

COPY *.js package*.json functions ${LAMBDA_TASK_ROOT}/
COPY functions ${LAMBDA_TASK_ROOT}/functions
RUN npm install
RUN chmod -R a+rX ${LAMBDA_TASK_ROOT}