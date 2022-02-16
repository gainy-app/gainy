CREATE SCHEMA IF NOT EXISTS meltano;
CREATE SCHEMA IF NOT EXISTS airflow;
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS deployment;

create table if not exists raw_data.eod_intraday_prices
(
    symbol      varchar,
    time        timestamp,
    open        double precision,
    high        double precision,
    low         double precision,
    close       double precision,
    volume      double precision,
    granularity int,
    constraint intraday_prices_pk
        primary key (time, symbol)
);

create table if not exists raw_data.auto_ticker_industries
(
    symbol              varchar,
    industry_id_1       int,
    industry_id_2       int,

    primary key (symbol)
);

create table if not exists deployment.public_schemas
(
    schema_name varchar   not null,
    deployed_at timestamp not null,
    deleted_at  int,

    primary key (schema_name)
);

insert into deployment.public_schemas(schema_name, deployed_at) values ('$DBT_TARGET_SCHEMA', now()) on conflict do update set deployed_at = excluded.deployed_at;