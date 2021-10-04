export PARAMS ?= $(filter-out $@,$(MAKECMDGOALS))

-include .env.make
-include .env

up:
	- cp -n src/gainy-fetch/meltano/symbols.local.json.dist src/gainy-fetch/meltano/symbols.local.json
	docker-compose up

upd:
	docker-compose up -d

config:
	docker-compose config

build:
	docker-compose build

down:
	docker-compose down

clean:
	docker-compose down --rmi local -v --remove-orphans

update: build update-quick

update-quick: clean up install

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

hasura:
	docker-compose exec -T hasura hasura $(PARAMS)

style-check:
	npx eslint src/aws/lambda-nodejs
	npx prettier --check "src/aws/lambda-nodejs/**/*.js"
	yapf --diff -r src/aws/lambda-python/

style-fix:
	npx eslint src/aws/lambda-nodejs --fix
	npx prettier --write "src/aws/lambda-nodejs/**/*.js"
	yapf -i -r src/aws/lambda-python/

extract-passwords:
	cd terraform && terraform state pull | python ../extract_passwords.py

test:
	docker-compose -p gainy_test -f docker-compose.test.yml run --rm test-meltano invoke dbt test
	make test-clean

test-clean:
	docker-compose -p gainy_test -f docker-compose.test.yml down --rmi local -v
	docker-compose -p gainy_test -f docker-compose.test.yml rm -sv
%:
	@:
