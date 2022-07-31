{{
  config(
    materialized = "incremental",
    unique_key = "contract_name",
    post_hook=[
      pk('contract_name'),
      index(this, 'symbol', false),
      'delete from {{this}} where expiration_date < now()::date',
    ]
  )
}}


with
{% if is_incremental() %}
     max_updated_at as (select symbol, max(_sdc_extracted_at) as max__sdc_extracted_at from {{ this }}),
{% endif %}
expanded as
    (
        select code                                                  as symbol,
               json_array_elements((json_each(options::json)).value) as value,
               base_tickers.name                                     as ticker_name,
               eod_options._sdc_extracted_at
        from {{ source('eod', 'eod_options') }}
                 join {{ ref('base_tickers') }} on eod_options.code = base_tickers.symbol
        where json_extract_path(options::json, 'CALL') is not null
           or json_extract_path(options::json, 'PUT') is not null
    )
select distinct on (
       (value ->> 'contractName')::varchar
    )  expanded.symbol,
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
        INITCAP((value ->> 'type')))::varchar  as name,
       expanded._sdc_extracted_at
from expanded
{% if is_incremental() %}
         left join max_updated_at on expanded.symbol = max_updated_at.symbol
{% endif %}
where (value ->> 'contractName')::varchar != '' and (value ->> 'contractName')::varchar is not null
  and (value ->> 'expirationDate')::date >= now()::date
{% if is_incremental() %}
  and expanded._sdc_extracted_at >= max_updated_at.max__sdc_extracted_at
{% endif %}