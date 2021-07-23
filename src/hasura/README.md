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

# Task 1 - setup authorization and authentication for Hasura

## The problem

Currently our auth service authentication is very simple - just uses single secret key passed in the header. 
This is not enough for a multi-user application we are building.

## Solution

We need to add a layer of authentication to our service. We are planning to use [Google Firebase](https://firebase.google.com/?gclsrc=ds&gclsrc=ds) and willing to investigate the intergration with it.

Please [read up this article](https://hasura.io/blog/authentication-and-authorization-using-hasura-and-firebase/) and build a working prototype which then we could promote to production.
