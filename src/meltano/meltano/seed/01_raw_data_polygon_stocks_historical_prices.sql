create table raw_data.polygon_stocks_historical_prices
(
    _sdc_batched_at   timestamp,
    _sdc_deleted_at   varchar,
    _sdc_extracted_at timestamp,
    c                 double precision,
    h                 double precision,
    l                 double precision,
    n                 numeric,
    o                 double precision,
    symbol            varchar not null,
    t                 numeric not null,
    v                 double precision,
    vw                double precision,
    primary key (t, symbol)
);
