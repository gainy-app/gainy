CREATE SCHEMA IF NOT EXISTS meltano;
CREATE SCHEMA IF NOT EXISTS airflow;
CREATE SCHEMA IF NOT EXISTS raw_data;

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
        primary key (symbol, time)
);
