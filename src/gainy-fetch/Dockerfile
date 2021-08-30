FROM meltano/meltano:v1.79.1 AS production

ENV PORT=5000

WORKDIR /project

COPY meltano /project
RUN rm -rf /project/.meltano
COPY tap-eodhistoricaldata /tap-eodhistoricaldata
RUN meltano install

EXPOSE $PORT

COPY docker-entrypoint.sh /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]


FROM production AS local
COPY meltano/meltano.local.yml /project/meltano.yml
