FROM meltano/meltano:v1.78.0

ENV PORT=5000

WORKDIR /project

COPY meltano /project
RUN rm -rf /project/.meltano
COPY docker-entrypoint.sh docker-entrypoint.sh
COPY tap-eodhistoricaldata /tap-eodhistoricaldata
RUN meltano install

EXPOSE $PORT

ENTRYPOINT ["./docker-entrypoint.sh"]
