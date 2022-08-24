create table if not exists raw_data.eod_dividends
(
    code              varchar not null,
    currency          varchar,
    date              varchar not null,
    declarationdate   varchar,
    paymentdate       varchar,
    period            varchar,
    recorddate        varchar,
    unadjustedvalue   double precision,
    value             double precision,
    _sdc_batched_at   timestamp,
    _sdc_deleted_at   varchar,
    _sdc_extracted_at timestamp,
    constraint dividends_pkey
        primary key (code, date)
);
