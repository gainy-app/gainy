FROM meltano/meltano:v1.78.0

ENV PORT=5000

WORKDIR /project

COPY meltano /project
RUN rm -rf /project/.meltano
COPY tap-eodhistoricaldata /tap-eodhistoricaldata
RUN meltano install

EXPOSE $PORT

COPY docker-entrypoint.sh /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
