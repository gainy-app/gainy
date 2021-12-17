{% snapshot eod_options_snapshots %}

{{
    config(
      target_schema='public',
      unique_key='code',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

/* todo investigate why there is no pk in resulting table */
/* todo change target_schema to variable */

select code,
       expirationdate,
       impliedvolatility,
       putcallopeninterestratio,
       putcallvolumeratio,
       callopeninterest,
       callvolume,
       optionscount,
       putopeninterest,
       putvolume,
       options,
       now()::date as updated_at
from {{ source('eod', 'eod_options') }}

{% endsnapshot %}