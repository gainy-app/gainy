{% snapshot historical_options_prices %}

{{
    config(
      target_schema='public',
      unique_key='contract_name',
      strategy='timestamp',
      updated_at='datetime',
    )
}}
/* todo change target_schema to variable */

select contract_name,
       last_price,
       last_trade_datetime::date as datetime
from {{ ref('ticker_options') }}

{% endsnapshot %}