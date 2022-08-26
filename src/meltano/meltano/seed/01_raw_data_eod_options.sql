create table if not exists raw_data.eod_options
(
    code                     varchar not null,
    callopeninterest         numeric,
    callvolume               numeric,
    expirationdate           varchar not null,
    impliedvolatility        double precision,
    optionscount             numeric,
    putcallopeninterestratio double precision,
    putcallvolumeratio       double precision,
    putopeninterest          numeric,
    putvolume                numeric,
    options                  jsonb,
    _sdc_batched_at          timestamp,
    _sdc_deleted_at          varchar,
    _sdc_extracted_at        timestamp,
    constraint options_pkey
        primary key (code, expirationdate)
);
