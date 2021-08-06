FROM hasura/graphql-engine:v2.0.1

RUN apt update
RUN apt install -y curl
RUN curl -L https://github.com/hasura/graphql-engine/raw/stable/cli/get.sh | bash

COPY hasura /hasura
WORKDIR /hasura

CMD graphql-engine serve --server-port $PORT