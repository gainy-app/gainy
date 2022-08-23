{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('symbol, date'),
      index(this, 'id', true),
      'delete from {{this}} where open_at < now() - interval \'1 week\'',
    ]
  )
}}


with trading_sessions as
         (
             select date,
                    exchange_name,
                    country_name,
                    open_at,
                    close_at,
                    row_number() over (partition by exchange_name, country_name order by date desc) - 1 as index,
                    null                                                                                as type
             from {{ ref('exchange_schedule') }}
             where open_at between now() - interval '1 week' and now()

             union all

             select dd::date                                 as date,
                    null                                     as exchange_name,
                    null                                     as country_name,
                    dd::date::timestamp                      as open_at,
                    dd::date::timestamp + interval '1 day'   as close_at,
                    row_number() over (order by dd desc) - 1 as index,
                    'crypto'                                 as type
             FROM generate_series(now() - interval '1 week', now(), interval '1 day') dd
         ),
     symbols as
         (
             select symbol,
                    type,
                    exchange_canonical,
                    country_name
             from {{ ref('base_tickers') }}

             union

             select contract_name as symbol,
                    'derivative'  as type,
                    exchange_canonical,
                    country_name
             from {{ ref('ticker_options_monitored') }}
                      join {{ ref('base_tickers') }} using (symbol)
         )
select t.*,
       now() as updated_at
from (
         select (symbols.symbol || '_' || trading_sessions.date) as id,
                symbols.symbol,
                trading_sessions.date,
                trading_sessions.index::int,
                trading_sessions.open_at,
                trading_sessions.close_at
         from symbols
                  join trading_sessions
                       on case
                              when symbols.type = 'crypto'
                                  then trading_sessions.type = 'crypto'
                              when symbols.exchange_canonical is not null
                                  then trading_sessions.exchange_name = symbols.exchange_canonical
                              else trading_sessions.country_name = symbols.country_name
                           end
     ) t
{% if is_incremental() %}
         left join {{ this }} old_trading_sessions using (symbol, date, index)
where old_trading_sessions is null
{% endif %}
