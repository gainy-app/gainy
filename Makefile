export PARAMS ?= $(filter-out $@,$(MAKECMDGOALS))

-include .env.make
-include .env

up:
	- cp -n src/gainy-fetch/meltano/symbols.local.json.dist src/gainy-fetch/meltano/symbols.local.json
	- docker-compose pull --include-deps
	docker-compose up

upd:
	docker-compose up -d

build:
	docker-compose build --parallel

down:
	docker-compose down

clean:
	docker-compose down --rmi local -v --remove-orphans

tf-fmt:
	cd terraform && terraform fmt -recursive

tf-init:
	cd terraform && terraform init

tf-plan:
	cd terraform && source .env && terraform plan $(PARAMS)

tf-apply:
	cd terraform && source .env && terraform apply -auto-approve $(PARAMS)

hasura-console:
	docker-compose exec -T hasura hasura console --address 0.0.0.0

hasura-seed:
	docker-compose exec -T hasura hasura seed apply

style-check:
	npx eslint src/aws/lambda-nodejs
	npx prettier --check "src/aws/lambda-nodejs/**/*.js"
	yapf --diff -r src/aws/lambda-python/ src/aws/router src/websockets src/gainy-fetch/meltano/orchestrate/dags terraform

style-fix:
	npx eslint src/aws/lambda-nodejs --fix
	npx prettier --write "src/aws/lambda-nodejs/**/*.js"
	yapf -i -r src/aws/lambda-python/ src/aws/router src/websockets src/gainy-fetch/meltano/orchestrate/dags terraform

extract-passwords:
	cd terraform && terraform state pull | python3 ../extract_passwords.py

test:
	docker-compose -p gainy_test -f docker-compose.test.yml run test-meltano invoke dbt test
	docker-compose -p gainy_test -f docker-compose.test.yml run --entrypoint python3 test-meltano tests/image_urls.py
	docker-compose -p gainy_test -f docker-compose.test.yml exec test-hasura pytest
	make test-clean

test-clean:
	docker-compose -p gainy_test -f docker-compose.test.yml down --rmi local -v
	docker-compose -p gainy_test -f docker-compose.test.yml rm -sv
%:
	@:
