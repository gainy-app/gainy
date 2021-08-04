# Gainy API server

This repo contains Gainy's GraphQL API server built on top of [Hasura](https://hasura.io/)

How to run the API server locally
1. [Install docker](https://docs.docker.com/get-docker/)
2. Open your terminal and run 
```bash
$ git clone https://github.com/gainy-app/gainy-etl # to checkout the repo
$ cd gainy-etl/src/hasura
$ docker-compose up
```

After this an API should be available on http://localhost:8080. You'll need a secret to login. Use `myadminsecretkey`.

Feel free to play around and create tables then explore the API via web console
