export PARAMS ?= $(filter-out $@,$(MAKECMDGOALS))

# workaround for setting env vars from script
_ := $(shell find .makeenv -mtime +12h -delete)
ifeq ($(shell test -e .makeenv && echo -n yes),)
	_ := $(shell . deployment/scripts/code_artifactory.sh; env | sed 's/=/:=/' | sed 's/^/export /' > .makeenv)
endif
include .makeenv
include .env.make
IMAGE_TAG ?= "latest"

docker-auth:
	- aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${BASE_IMAGE_REGISTRY_ADDRESS}

env:
	touch .env.local
	- if [ ! -f src/meltano/meltano/exchanges.local.json ]; then cp -n src/meltano/meltano/symbols.local.json.dist src/meltano/meltano/symbols.local.json; fi

configure: clean docker-auth env build
	- docker network create gainy-default

up:
	docker-compose up --no-build

upd:
	docker-compose up -d

build:
	docker-compose build

down:
	docker-compose down

clean:
	- docker-compose down --rmi local -v --remove-orphans

hasura-console:
	docker-compose exec -T hasura hasura console --address 0.0.0.0 --api-host http://0.0.0.0 --endpoint http://0.0.0.0:8080 --no-browser --skip-update-check

start-realtime:
	@echo -n 'Enter provider (eod, polygon): ' && read provider && docker-compose up websockets-$${provider} --scale websockets-$${provider}=1

style-check:
	yapf --diff -r src/aws/lambda-python/ src/aws/router src/websockets src/meltano/meltano/orchestrate/dags src/hasura src/meltano terraform

style-fix:
	yapf -i -r src/aws/lambda-python/ src/aws/router src/websockets src/meltano/meltano/orchestrate/dags src/hasura src/meltano terraform
	cd terraform && terraform fmt -recursive

extract-passwords:
	cd terraform && terraform state pull | python3 ../extract_passwords.py

ci-build:
	- docker run -d -p 5123:5000 --restart=always --name registry -v /tmp/docker-registry:/var/lib/registry registry:2 && npx wait-on tcp:5123
	- docker pull localhost:5123/gainy-meltano:${BASE_IMAGE_VERSION}
	docker pull ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-meltano:${BASE_IMAGE_VERSION}
	docker tag ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-meltano:${BASE_IMAGE_VERSION} localhost:5123/gainy-meltano:${BASE_IMAGE_VERSION}
	docker push localhost:5123/gainy-meltano:${BASE_IMAGE_VERSION}

	- docker pull localhost:5123/gainy-meltano:${IMAGE_TAG}
	docker build ./src/meltano -t gainy-meltano:${IMAGE_TAG} --cache-from=localhost:5123/gainy-meltano:${IMAGE_TAG} --build-arg BASE_IMAGE_REGISTRY_ADDRESS=${BASE_IMAGE_REGISTRY_ADDRESS} --build-arg BASE_IMAGE_VERSION=${BASE_IMAGE_VERSION} --build-arg CODEARTIFACT_PIPY_URL=${CODEARTIFACT_PIPY_URL} --build-arg GAINY_COMPUTE_VERSION=${GAINY_COMPUTE_VERSION}
	docker tag gainy-meltano:${IMAGE_TAG} localhost:5123/gainy-meltano:${IMAGE_TAG}
	docker push localhost:5123/gainy-meltano:${IMAGE_TAG}

	- docker pull localhost:5123/gainy-hasura:${BASE_IMAGE_VERSION}
	docker pull ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-hasura:${BASE_IMAGE_VERSION}
	docker tag ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-hasura:${BASE_IMAGE_VERSION} localhost:5123/gainy-hasura:${BASE_IMAGE_VERSION}
	docker push localhost:5123/gainy-hasura:${BASE_IMAGE_VERSION}

	- docker pull localhost:5123/gainy-hasura:${IMAGE_TAG}
	docker build ./src/hasura -t gainy-hasura:${IMAGE_TAG} --cache-from=localhost:5123/gainy-hasura:${IMAGE_TAG} --build-arg BASE_IMAGE_REGISTRY_ADDRESS=${BASE_IMAGE_REGISTRY_ADDRESS} --build-arg BASE_IMAGE_VERSION=${BASE_IMAGE_VERSION} --build-arg CODEARTIFACT_PIPY_URL=${CODEARTIFACT_PIPY_URL} --build-arg GAINY_COMPUTE_VERSION=${GAINY_COMPUTE_VERSION}
	docker tag gainy-hasura:${IMAGE_TAG} localhost:5123/gainy-hasura:${IMAGE_TAG}
	docker push localhost:5123/gainy-hasura:${IMAGE_TAG}

	- docker pull localhost:5123/gainy-lambda-python:${BASE_IMAGE_VERSION}
	docker pull ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-lambda-python:${BASE_IMAGE_VERSION}
	docker tag ${BASE_IMAGE_REGISTRY_ADDRESS}/gainy-lambda-python:${BASE_IMAGE_VERSION} localhost:5123/gainy-lambda-python:${BASE_IMAGE_VERSION}
	docker push localhost:5123/gainy-lambda-python:${BASE_IMAGE_VERSION}

	- docker pull localhost:5123/gainy-lambda-python:${IMAGE_TAG}
	docker build ./src/aws/lambda-python -t gainy-lambda-python:${IMAGE_TAG} --cache-from=localhost:5123/gainy-lambda-python:${IMAGE_TAG} --build-arg BASE_IMAGE_REGISTRY_ADDRESS=${BASE_IMAGE_REGISTRY_ADDRESS} --build-arg BASE_IMAGE_VERSION=${BASE_IMAGE_VERSION} --build-arg CODEARTIFACT_PIPY_URL=${CODEARTIFACT_PIPY_URL} --build-arg GAINY_COMPUTE_VERSION=${GAINY_COMPUTE_VERSION}
	docker tag gainy-lambda-python:${IMAGE_TAG} localhost:5123/gainy-lambda-python:${IMAGE_TAG}
	docker push localhost:5123/gainy-lambda-python:${IMAGE_TAG}

test-meltano:
	docker-compose -p gainy_test -f docker-compose.test.yml run --rm test-meltano invoke dbt test
	docker-compose -p gainy_test -f docker-compose.test.yml run --rm --entrypoint gainy_recommendation test-meltano

test-images:
	docker-compose -p gainy_test -f docker-compose.test.yml run --rm --entrypoint python3 test-meltano tests/image_urls.py

test-meltano-realtime:
	docker-compose -p gainy_test -f docker-compose.test.yml run --rm --entrypoint "/wait.sh" test-meltano invoke dbt run --exclude tag:view
	docker-compose -p gainy_test -f docker-compose.test.yml run --rm --entrypoint "/wait.sh" test-meltano invoke dbt run --vars '{"realtime": true}' --select tag:realtime
	docker-compose -p gainy_test -f docker-compose.test.yml run --rm --entrypoint "/wait.sh" test-meltano invoke dbt test

test-hasura:
	docker-compose -p gainy_test -f docker-compose.test.yml exec -T test-hasura pytest

test-lambda:
	docker-compose -p gainy_test -f docker-compose.test.yml exec -T test-lambda-python-action pytest

test-configure: test-clean docker-auth env ci-build

test: test-configure test-meltano test-images test-meltano-realtime test-hasura test-lambda test-clean

test-clean:
	docker-compose -p gainy_test -f docker-compose.test.yml down --rmi local -v
	docker-compose -p gainy_test -f docker-compose.test.yml rm -sv
%:
	@:
