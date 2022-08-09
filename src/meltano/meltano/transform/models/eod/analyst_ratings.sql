{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}


with expanded as
    (
         select distinct code::text                       as symbol,
                (analystratings ->> 'Buy')::int           as buy,
                (analystratings ->> 'Hold')::int          as hold,
                (analystratings ->> 'Sell')::int          as sell,
                (analystratings ->> 'Rating')::float      as rating,
                (analystratings ->> 'StrongBuy')::int     as strong_buy,
                (analystratings ->> 'StrongSell')::int    as strong_sell,
                (analystratings ->> 'TargetPrice')::float as target_price,
                case
                    when is_date(updatedat)
                        then updatedat::timestamp
                    else _sdc_batched_at
                    end                                   as updated_at
         from {{ source('eod', 'eod_fundamentals') }}
    )
select expanded.*
from expanded
{% if is_incremental() %}
         left join {{ this }} old_data using (symbol)
{% endif %}
where (expanded.buy is not null
   or expanded.hold is not null
   or expanded.sell is not null
   or expanded.rating is not null
   or expanded.strong_buy is not null
   or expanded.strong_sell is not null
   or expanded.target_price is not null)
{% if is_incremental() %}
  and expanded.updated_at >= old_data.updated_at or old_data is null
{% endif %}
