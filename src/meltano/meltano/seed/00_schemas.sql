CREATE SCHEMA IF NOT EXISTS meltano;
CREATE SCHEMA IF NOT EXISTS airflow;
CREATE SCHEMA IF NOT EXISTS mlflow;
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS deployment;

create table if not exists raw_data.eod_intraday_prices
(
    symbol      varchar,
    time        timestamp,
    open        numeric,
    high        numeric,
    low         numeric,
    close       numeric,
    volume      numeric,
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
alter table raw_data.auto_ticker_industries add if not exists industry_1_cossim double precision;
alter table raw_data.auto_ticker_industries add if not exists industry_2_cossim double precision;
alter table raw_data.auto_ticker_industries add if not exists min_cossim double precision;

create table if not exists deployment.public_schemas
(
    schema_name varchar   not null,
    deployed_at timestamp not null,
    deleted_at  timestamp,

    primary key (schema_name)
);

insert into deployment.public_schemas(schema_name, deployed_at) values ('$DBT_TARGET_SCHEMA', now()) on conflict do update set deployed_at = excluded.deployed_at;

create table if not exists deployment.realtime_listener_heartbeat
(
    source        varchar   not null,
    key           varchar   not null,
    symbols_count int       not null,
    time          timestamp not null,

    primary key (time, source, key)
);
