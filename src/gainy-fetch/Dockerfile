FROM meltano/meltano

WORKDIR /project

# Generate requirements and install *all* dependencies.
COPY meltano /project
COPY docker-entrypoint.sh docker-entrypoint.sh
COPY tap-eodhistoricaldata /tap-eodhistoricaldata
RUN meltano install

ENTRYPOINT ["./docker-entrypoint.sh"]