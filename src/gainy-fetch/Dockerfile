FROM meltano/meltano:v1.80.1-python3.8 AS local

RUN apt update && apt install -y postgresql-client
RUN pip install pyyaml

WORKDIR /project

COPY meltano /project
RUN rm -rf /project/.meltano

ENV ENV=local
ENV EODHISTORICALDATA_JOBS_COUNT=1
RUN cp -n symbols.local.json.dist symbols.local.json
RUN python scripts/generate_meltano_config.py $ENV

COPY tap-eodhistoricaldata /tap-eodhistoricaldata
RUN cd /tap-eodhistoricaldata && pip install . # for some reason install below fails on heroku and hangs locally without this line
RUN meltano install

COPY init.sh wait.sh /
RUN chmod +x /init.sh /wait.sh

FROM local AS production
ENV ENV=production
RUN python scripts/generate_meltano_config.py $ENV
