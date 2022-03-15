export PARAMS ?= $(filter-out $@,$(MAKECMDGOALS))

# workaround for setting env vars from script
_ := $(shell bash -c "source deployment/scripts/code_artifactory.sh; env | sed 's/=/:=/' | sed 's/^/export /' > .makeenv")
include .makeenv
include .env.make

docker-auth:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${BASE_IMAGE_REGISTRY_ADDRESS}

configure: clean docker-auth
	touch .env.local
	- if [ ! -f src/meltano/meltano/exchanges.local.json ]; then cp -n src/meltano/meltano/symbols.local.json.dist src/meltano/meltano/symbols.local.json; fi
	- docker network create gainy-default

up:
	- docker-compose pull --include-deps
	docker-compose up

upd:
	docker-compose up -d

build:
	docker-compose build --parallel --no-cache --progress plain

down:
	docker-compose down

clean:
	- docker-compose down --rmi local -v --remove-orphans

hasura-console:
	docker-compose exec -T hasura hasura console --address 0.0.0.0 --api-host http://0.0.0.0 --endpoint http://0.0.0.0:8080 --no-browser --skip-update-check

start-realtime:
	@echo -n 'Enter provider (eod, polygon): ' && read provider && docker-compose up websockets_$${provider} --scale websockets_$${provider}=1

style-check:
	yapf --diff -r src/aws/lambda-python/ src/aws/router src/websockets src/meltano/meltano/orchestrate/dags src/hasura src/meltano terraform

style-fix:
	yapf -i -r src/aws/lambda-python/ src/aws/router src/websockets src/meltano/meltano/orchestrate/dags src/hasura src/meltano terraform
	cd terraform && terraform fmt -recursive

extract-passwords:
	cd terraform && terraform state pull | python3 ../extract_passwords.py

test-build:
	docker-compose -p gainy_test -f docker-compose.test.yml build --no-cache --progress plain

test-init:
	docker-compose -p gainy_test -f docker-compose.test.yml run test-meltano invoke dbt test

test-images:
	docker-compose -p gainy_test -f docker-compose.test.yml run --entrypoint python3 test-meltano tests/image_urls.py

test-realtime:
	docker-compose -p gainy_test -f docker-compose.test.yml run --entrypoint "/wait.sh" test-meltano invoke dbt run --vars '{"realtime": true}' --model historical_prices_aggregated portfolio_gains portfolio_holding_details portfolio_holding_gains portfolio_holding_group_details portfolio_holding_group_gains portfolio_transaction_chart portfolio_expanded_transactions
	docker-compose -p gainy_test -f docker-compose.test.yml run --entrypoint "/wait.sh" test-meltano invoke dbt test

test-hasura:
	docker-compose -p gainy_test -f docker-compose.test.yml exec -T test-hasura pytest

test: configure test-build test-init test-images test-realtime test-hasura test-clean

test-clean:
	docker-compose -p gainy_test -f docker-compose.test.yml down --rmi local -v
	docker-compose -p gainy_test -f docker-compose.test.yml rm -sv
%:
	@:
