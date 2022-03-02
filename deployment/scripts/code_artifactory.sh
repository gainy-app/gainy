#!/bin/bash

# CODEARTIFACT
CODEARTIFACT_AUTH_TOKEN="$(aws codeartifact get-authorization-token --domain gainy-app --query authorizationToken --output text)"
CODEARTIFACT_REPO_URL="$(aws codeartifact get-repository-endpoint --domain gainy-app --repository gainy-app --format pypi --query repositoryEndpoint --output text | sed 's/https:\/\///g' )"
CODEARTIFACT_PIPY_URL=https://aws:${CODEARTIFACT_AUTH_TOKEN}@${CODEARTIFACT_REPO_URL}simple/

export CODEARTIFACT_PIPY_URL