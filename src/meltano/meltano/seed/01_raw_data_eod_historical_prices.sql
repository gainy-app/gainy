create table if not exists raw_data.eod_historical_prices
(
    code              varchar not null,
    adjusted_close    double precision,
    close             double precision,
    date              varchar not null,
    high              double precision,
    low               double precision,
    open              double precision,
    volume            numeric,
    _sdc_batched_at   timestamp,
    _sdc_deleted_at   varchar,
    _sdc_extracted_at timestamp,
    constraint historical_prices_pkey
        primary key (code, date)
);
