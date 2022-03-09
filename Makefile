export PARAMS ?= $(filter-out $@,$(MAKECMDGOALS))

-include .env.make
-include .env

docker-auth:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${BASE_IMAGE_REGISTRY_ADDRESS}

configure: docker-auth
	- if [ ! -f src/gainy-fetch/meltano/exchanges.local.json ]; then cp -n src/gainy-fetch/meltano/symbols.local.json.dist src/gainy-fetch/meltano/symbols.local.json; fi

up: configure
	- docker-compose pull --include-deps
	docker-compose up

upd:
	docker-compose up -d

build:
	docker-compose build --parallel

down:
	docker-compose down

clean:
	- docker-compose down --rmi local -v --remove-orphans

tf-fmt:
	cd terraform && terraform fmt -recursive

tf-init:
	cd terraform && terraform init

tf-plan:
	cd terraform && source .env && terraform plan $(PARAMS)

tf-apply:
	cd terraform && source .env && terraform apply -auto-approve $(PARAMS)

hasura-console:
	docker-compose exec -T hasura hasura console --address 0.0.0.0 --api-host http://0.0.0.0 --endpoint http://0.0.0.0:8080 --no-browser --skip-update-check

hasura-seed:
	docker-compose exec -T hasura hasura seed apply

style-check:
	yapf --diff -r src/aws/lambda-python/ src/aws/router src/websockets src/gainy-fetch/meltano/orchestrate/dags src/hasura src/gainy-fetch terraform

style-fix:
	yapf -i -r src/aws/lambda-python/ src/aws/router src/websockets src/gainy-fetch/meltano/orchestrate/dags src/hasura src/gainy-fetch terraform

extract-passwords:
	cd terraform && terraform state pull | python3 ../extract_passwords.py

test-build:
	docker-compose -p gainy_test -f docker-compose.test.yml build

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
