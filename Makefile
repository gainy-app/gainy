export PARAMS ?= $(filter-out $@,$(MAKECMDGOALS))

-include .env

install:
	#wait for postgresql to start
	docker-compose exec meltano bash -c 'while !</dev/tcp/postgres/5432; do sleep 1; done;'
	sleep 3
    # FIXME: figure out why --transform=run does not run the dbt models locally
	docker-compose exec meltano meltano schedule run eodhistoricaldata-to-postgres

up:
	docker-compose up -d

build:
	docker-compose build

down:
	docker-compose down

clean:
	docker-compose down -v

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

%:
	@:
