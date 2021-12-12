{{
  config(
    materialized = "incremental",
    unique_key = "contract_name",
    post_hook=[
      index(this, 'contract_name', true),
      index(this, 'symbol', false),
    ]
  )
}}


with
{% if is_incremental() %}
     max_updated_at as (select symbol, max(updated_at) as max_date from ticker_options group by symbol),
{% endif %}
expanded as
    (
        select code                                                  as symbol,
               json_array_elements((json_each(options::json)).value) as value

        from {{ source('eod', 'eod_options') }} f
                 inner join {{ ref('tickers') }} as t
                            on f.code = t.symbol
    )
select expanded.symbol,
       (value ->> 'ask')::float                as ask,
       (value ->> 'bid')::float                as bid,
       (value ->> 'change')::float             as change,
       (value ->> 'changePercent')::float      as change_percent,
       (value ->> 'contractName')::varchar     as contract_name,
       (value ->> 'contractPeriod')::varchar   as contract_period,
       (value ->> 'contractSize')::varchar     as contract_size,
       (value ->> 'currency')::varchar         as currency,
       (value ->> 'daysBeforeExpiration')::int as days_before_expiration,
       (value ->> 'delta')::float              as delta,
       (value ->> 'expirationDate')::date      as expiration_date,
       (value ->> 'gamma')::float              as gamma,
       (value ->> 'impliedVolatility')::float  as implied_volatility,
       lower(value ->> 'inTheMoney') = 'true'  as in_the_money,
       (value ->> 'intrinsicValue')::float     as intrinsic_value,
       (value ->> 'lastPrice')::float          as last_price,
       case
           when (value ->> 'lastTradeDateTime') != '0000-00-00 00:00:00'
               then (value ->> 'lastTradeDateTime')::timestamp
           end                                 as last_trade_datetime,
       (value ->> 'openInterest')::int         as open_interest,
       (value ->> 'rho')::float                as rho,
       (value ->> 'strike')::float             as strike,
       (value ->> 'theoretical')::float        as theoretical,
       (value ->> 'theta')::float              as theta,
       (value ->> 'timeValue')::float          as time_value,
       (value ->> 'type')::varchar             as type,
       (value ->> 'updatedAt')::timestamp      as updated_at,
       (value ->> 'vega')::float               as vega,
       (value ->> 'volume')::int               as volume,
       (expanded.symbol || ' ' ||
       to_char((value ->> 'expirationDate')::date, 'MM/dd/YYYY') || ' ' ||
       (value ->> 'strike') || ' ' ||
       INITCAP((value ->> 'type')))::varchar   as name
from expanded
{% if is_incremental() %}
         left join max_updated_at on expanded.symbol = max_updated_at.symbol
where (value ->> 'updatedAt')::timestamp >= max_updated_at.max_date
   or max_updated_at.max_date is null
{% endif %}