FROM meltano/meltano:v1.77.0

WORKDIR /project

COPY meltano /project
RUN rm /project/.meltano
COPY docker-entrypoint.sh docker-entrypoint.sh
COPY tap-eodhistoricaldata /tap-eodhistoricaldata
RUN meltano install

ENTRYPOINT ["./docker-entrypoint.sh"]
