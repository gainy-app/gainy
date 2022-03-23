{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "profile_id__date__period" ON {{ this }} (profile_id, date, period)',
      'delete from {{this}} where updated_at is null or updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

with week_trading_sessions as
         (
             select min(open_at)                           as open_at,
                    min(close_at)                          as close_at,
                    min(date)                              as date,
                    row_number() over (order by date desc) as idx
             from {{ ref('exchange_schedule') }}
             where open_at between now() - interval '1 week' and now()
             group by date
         ),
     latest_open_trading_session as
         (
             select *
             from week_trading_sessions
             where idx = 1
         ),
     static_values as
         (
             select profile_id,
                    sum(
                            case
                                when ticker_options.contract_name is not null
                                    then 100 * profile_holdings_normalized.quantity::numeric * ticker_options.last_price::numeric
                                when portfolio_securities_normalized.type = 'cash'
                                    and portfolio_securities_normalized.ticker_symbol = 'CUR:USD'
                                    then profile_holdings_normalized.quantity::numeric
                                else 0
                                end
                        ) as value
             from {{ ref('profile_holdings_normalized') }}
                      join {{ ref('portfolio_securities_normalized') }}
                           on portfolio_securities_normalized.id = profile_holdings_normalized.security_id
                      left join {{ ref('ticker_options') }}
                                on ticker_options.contract_name = portfolio_securities_normalized.original_ticker_symbol
             group by profile_id
         ),
     dynamic_values as
         (
             select profile_id,
                    period,
                    portfolio_transaction_chart.datetime,
                    count(uniq_id)::double precision as transaction_count,
                    sum(adjusted_close::numeric)     as value
             from {{ ref('portfolio_transaction_chart') }}
                      join {{ ref('portfolio_expanded_transactions') }}
                           on portfolio_expanded_transactions.uniq_id = portfolio_transaction_chart.transactions_uniq_id
                      left join week_trading_sessions
                                on portfolio_transaction_chart.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
                      left join latest_open_trading_session on true
             where ((period = '1d' and week_trading_sessions.idx = 1)
                 or (period = '1w' and week_trading_sessions.idx is not null)
                 or (period = '1m' and
                     portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '1 month')
                 or (period = '3m' and
                     portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '3 months')
                 or (period = '1y' and
                     portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '1 year')
                 or (period = '5y' and
                     portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '5 years')
                 or (period = 'all'))
             group by profile_id, period, portfolio_transaction_chart.datetime
         ),
     dynamic_values_extended as
         (
             select profile_id,
                    case
                        when period = '1d' then '15min'
                        when period = '1m' then '1d'
                        when period = '5y' then '1w'
                        when period = 'all' then '1m'
                        end::varchar                                                                                  as period,
                    datetime,
                    (profile_id || '_' || datetime || '_' || period)::varchar                                         as id,
                    value,
                    transaction_count,
                    first_value(transaction_count)
                    over (partition by profile_id, period order by datetime rows between 1 preceding and current row) as prev_transaction_count
             from dynamic_values
             where (period = '1d' and mod(date_part('minute', datetime)::int, 15) = 0)
                or (period = '1m' and datetime > now() - interval '1 week')
                or (period = '5y' and datetime > now() - interval '1 year')
                or period = 'all'
         )
select dynamic_values_extended.profile_id,
       period,
       datetime,
       datetime                                                                as date,
       id,
       (dynamic_values_extended.value + static_values.value)::double precision as value,
       now()                                                                   as updated_at
from dynamic_values_extended
         left join static_values on static_values.profile_id = dynamic_values_extended.profile_id
where transaction_count >= prev_transaction_count
