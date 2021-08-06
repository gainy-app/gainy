export PARAMS ?= $(filter-out $@,$(MAKECMDGOALS))

-include .env

install:
	docker-compose exec meltano meltano schedule run eodhistoricaldata-to-postgres-0 --transform=run
	docker-compose exec -T hasura hasura metadata apply

up:
	docker-compose up -d

build:
	docker-compose build

down:
	docker-compose down

update: build update-quick

update-quick: up install

plan:
	cd terraform && source .env && terraform plan

%:
	@:
