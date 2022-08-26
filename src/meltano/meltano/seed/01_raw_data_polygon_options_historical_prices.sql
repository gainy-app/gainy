create table if not exists raw_data.polygon_options_historical_prices
(
    _sdc_batched_at   timestamp,
    _sdc_deleted_at   varchar,
    _sdc_extracted_at timestamp,
    c                 double precision,
    contract_name     varchar not null,
    h                 double precision,
    l                 double precision,
    n                 numeric,
    o                 double precision,
    t                 numeric not null,
    v                 numeric,
    vw                double precision,
    primary key (t, contract_name)
);
