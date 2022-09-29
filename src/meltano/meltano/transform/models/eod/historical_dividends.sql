{{
  config(
    materialized = "table",
    post_hook=[
      pk('symbol, date'),
    ]
  )
}}

SELECT eod_dividends.code as symbol,
       eod_dividends.date::date,
       eod_dividends.currency,
       eod_dividends.period,
       eod_dividends.recorddate::date,
       eod_dividends.value::numeric,
       eod_dividends._sdc_extracted_at as updated_at
from {{ source('eod', 'eod_dividends') }}

{% if is_incremental() %}
    left join {{ this }} old_data
              on old_data.symbol = code
                  and old_data.date = date::date
{% endif %}

WHERE eod_dividends.code is not null
  and eod_dividends.value is not null
{% if is_incremental() %}
  and (old_data is null or abs(old_data.value - eod_dividends.value) > 1e-2)
{% endif %}
