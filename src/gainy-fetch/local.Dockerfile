FROM meltano/meltano:v1.78.0

ENV PORT=5000

WORKDIR /project

COPY meltano /project
COPY meltano/meltano.local.yml /project/meltano.yml
RUN rm -rf /project/.meltano
COPY tap-eodhistoricaldata /tap-eodhistoricaldata
RUN meltano install

EXPOSE $PORT

ENTRYPOINT ["meltano", "ui"]
