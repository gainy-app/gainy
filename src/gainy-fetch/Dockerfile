FROM meltano/meltano:v1.77.0

WORKDIR /project

COPY meltano /project
RUN rm -rf /project/.meltano
COPY docker-entrypoint.sh docker-entrypoint.sh
COPY tap-eodhistoricaldata /tap-eodhistoricaldata
RUN meltano install

CMD ["./docker-entrypoint.sh"]
