export PARAMS ?= $(filter-out $@,$(MAKECMDGOALS))

# workaround for setting env vars from script
_ := $(shell find .makeenv -mtime +12h -delete)
ifeq ($(shell test -e .makeenv && echo -n yes),)
	_ := $(shell . deployment/scripts/code_artifactory.sh; env | sed 's/=/:=/' | sed 's/^/export /' > .makeenv)
endif
include .makeenv
include .env.make

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

test-build-github:
	docker buildx build --cache-from "type=local,src=/tmp/.buildx-cache" --cache-to "type=local,dest=/tmp/.buildx-cache-new" ${BUILD_FLAGS} -t gainy-meltano:latest --build-arg BASE_IMAGE_REGISTRY_ADDRESS=${BASE_IMAGE_REGISTRY_ADDRESS} --build-arg BASE_IMAGE_VERSION=${BASE_IMAGE_VERSION} --build-arg CODEARTIFACT_PIPY_URL=${CODEARTIFACT_PIPY_URL} --build-arg GAINY_COMPUTE_VERSION=${GAINY_COMPUTE_VERSION} ./src/meltano

test-build:
	docker-compose -p gainy_test -f docker-compose.test.yml build --progress plain

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

test-configure: test-clean docker-auth env test-build

test: test-configure test-meltano test-images test-meltano-realtime test-hasura test-lambda test-clean

test-clean:
	docker-compose -p gainy_test -f docker-compose.test.yml down --rmi local -v
	docker-compose -p gainy_test -f docker-compose.test.yml rm -sv
%:
	@:
