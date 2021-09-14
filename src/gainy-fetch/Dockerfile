FROM meltano/meltano:v1.80.1-python3.8 AS production

ENV MELTANO_UI_PORT=5000
ENV AIRFLOW_UI_PORT=5001

WORKDIR /project

COPY meltano /project
RUN rm -rf /project/.meltano
COPY tap-eodhistoricaldata /tap-eodhistoricaldata
RUN cd /tap-eodhistoricaldata && pip install . # for some reason install below fails on heroku and hangs locally without this line
RUN meltano install

EXPOSE $MELTANO_UI_PORT
EXPOSE $AIRFLOW_UI_PORT

COPY docker-entrypoint.sh /docker-entrypoint.sh

CMD ["production"]
ENTRYPOINT ["/docker-entrypoint.sh"]


FROM production AS local
COPY meltano/meltano.local.yml /project/meltano.yml
COPY docker-entrypoint-local.sh /docker-entrypoint-local.sh
CMD ["local"]
ENTRYPOINT ["/docker-entrypoint-local.sh"]
