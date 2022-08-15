CREATE SCHEMA IF NOT EXISTS meltano;
CREATE SCHEMA IF NOT EXISTS airflow;
CREATE SCHEMA IF NOT EXISTS mlflow;
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS deployment;
CREATE SCHEMA IF NOT EXISTS gainy_history;

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
alter table deployment.public_schemas alter deployed_at drop not null;

create table if not exists deployment.realtime_listener_heartbeat
(
    source        varchar   not null,
    key           varchar   not null,
    symbols_count int       not null,
    time          timestamp not null,

    primary key (time, source, key)
);

create table gainy_history.collection_tickers_weighted
(
    _sdc_batched_at    timestamp,
    _sdc_deleted_at    varchar,
    _sdc_extracted_at  timestamp,
    collection_id      integer,
    collection_uniq_id varchar   not null,
    date               timestamp not null,
    profile_id         integer,
    symbol             varchar   not null,
    weight             numeric,
    primary key (collection_uniq_id, symbol, date)
);
