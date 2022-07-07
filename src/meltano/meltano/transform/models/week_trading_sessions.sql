{% set interval = '1 week' %}

{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, date'),
      index(this, 'id', true),
      'delete from {{this}} where open_at < now() - interval \'1 week\'',
    ]
  )
}}

with week_trading_sessions as (
    select *,
           row_number() over (partition by exchange_name, country_name order by date desc) - 1 as index
    from {{ ref('exchange_schedule') }}
    where open_at between now() - interval '1 week' and now()
)
select t.*
from (
         select (base_tickers.symbol || '_' || week_trading_sessions.date) as id,
                base_tickers.symbol,
                week_trading_sessions.date,
                week_trading_sessions.index,
                week_trading_sessions.open_at,
                week_trading_sessions.close_at,
                now()                                                      as updated_at
         from {{ ref('base_tickers') }}
                  join week_trading_sessions
                       on (week_trading_sessions.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            week_trading_sessions.country_name = base_tickers.country_name))

         union all

         select (ticker_options_monitored.contract_name || '_' || week_trading_sessions.date) as id,
                ticker_options_monitored.contract_name                                        as symbol,
                week_trading_sessions.date,
                week_trading_sessions.index,
                week_trading_sessions.open_at,
                week_trading_sessions.close_at,
                now()                                                                         as updated_at
         from {{ ref('ticker_options_monitored') }}
                  join {{ ref('base_tickers') }} using (symbol)
                  join week_trading_sessions
                       on (week_trading_sessions.exchange_name = base_tickers.exchange_canonical or
                           (base_tickers.exchange_canonical is null and
                            week_trading_sessions.country_name = base_tickers.country_name))
      ) t
{% if is_incremental() %}
         left join {{ this }} old_week_trading_sessions using (symbol, date, index)
where old_week_trading_sessions is null
{% endif %}