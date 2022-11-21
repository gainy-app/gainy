{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, date'),
      index('id', true),
    ]
  )
}}


with expanded as (
    select code    as symbol,
           (json_each((earnings -> 'History')::json)).*,
           case
               when is_date(updatedat)
                   then updatedat::timestamp
               else _sdc_batched_at
               end as updated_at
    from {{ source('eod', 'eod_fundamentals') }}
)
select expanded.symbol,
       expanded.key::date                              as date,
       (expanded.symbol || '_' || expanded.key)        as id,
       (expanded.value ->> 'currency')::text           as currency,
       (expanded.value ->> 'epsActual')::numeric       as eps_actual,
       (expanded.value ->> 'reportDate')::date         as report_date,
       (expanded.value ->> 'epsEstimate')::numeric     as eps_estimate,
       (expanded.value ->> 'epsDifference')::numeric   as eps_difference,
       (expanded.value ->> 'surprisePercent')::numeric as surprise_percent,
       (expanded.value ->> 'beforeAfterMarket')::text  as before_after_market,
       expanded.updated_at
from expanded
{% if is_incremental() %}
         left join {{ this }} old_data
                   on old_data.symbol = expanded.symbol
                       and old_data.expanded.key::date = expanded.date
{% endif %}

where key != '0000-00-00'

{% if is_incremental() %}
  and old_data.symbol is null
   or expanded.updated_at > old_data.updated_at
{% endif %}
