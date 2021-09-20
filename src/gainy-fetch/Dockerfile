FROM meltano/meltano:v1.80.1-python3.8 AS local

RUN apt update && apt install -y postgresql-client supervisor
RUN pip install pyyaml

ENV MELTANO_UI_PORT=5000
ENV AIRFLOW_UI_PORT=5001

WORKDIR /project

COPY supervisor.conf /etc/supervisor/conf.d

COPY meltano /project
RUN rm -rf /project/.meltano

ENV ENV=local
ENV EODHISTORICALDATA_JOBS_COUNT=1
RUN python scripts/generate_meltano_config.py $ENV

COPY tap-eodhistoricaldata /tap-eodhistoricaldata
RUN cd /tap-eodhistoricaldata && pip install . # for some reason install below fails on heroku and hangs locally without this line
RUN meltano install

EXPOSE $MELTANO_UI_PORT
EXPOSE $AIRFLOW_UI_PORT

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]

FROM local AS production
ENV ENV=production
RUN python scripts/generate_meltano_config.py $ENV
