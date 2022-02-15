{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "profile_id__date__period") }} (profile_id, date, period)',
      'delete from {{this}} where updated_at is null or updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

with
     first_transaction_date as
         (
             select profile_id,
                    min(date) as date
             from {{ source('app', 'profile_portfolio_transactions') }}
             group by profile_id
         ),
     time_period as
         (
             select distinct datetime, period
             from {{ ref('historical_prices_aggregated') }}
             where historical_prices_aggregated.period != '15min'
             union all
             select distinct datetime, '15min'::varchar as period
             from {{ ref('chart') }}
             where chart.period = '1d'
         ),
     chart_data as (
         -- stocks
         (
             select portfolio_expanded_transactions.profile_id,
                    historical_prices_aggregated.time                    as datetime,
                    historical_prices_aggregated.period                  as period,
                    portfolio_expanded_transactions.quantity_norm::numeric *
                    historical_prices_aggregated.adjusted_close::numeric as value,
                    'chart'                                              as source
             from {{ ref('portfolio_expanded_transactions') }}
                      join {{ ref('portfolio_securities_normalized') }}
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                      join {{ ref('historical_prices_aggregated') }}
                           on (historical_prices_aggregated.datetime >= portfolio_expanded_transactions.datetime or
                               portfolio_expanded_transactions.datetime is null) and
                              historical_prices_aggregated.symbol = portfolio_securities_normalized.original_ticker_symbol
                      left join first_transaction_date on first_transaction_date.profile_id = portfolio_expanded_transactions.profile_id
             where portfolio_expanded_transactions.type in ('buy', 'sell')
               and (first_transaction_date.profile_id is null or historical_prices_aggregated.time >= first_transaction_date.date)
               and historical_prices_aggregated.period != '15min'
         )

         union all

         -- realtime
         (
             with chart_dates as (
                 select distinct datetime
                 from {{ ref('chart') }}
                 where chart.period = '1d'
             )
             select distinct on (
                 portfolio_expanded_transactions.uniq_id,
                 chart_dates.datetime
                 ) portfolio_expanded_transactions.profile_id,
                   chart_dates.datetime,
                   '15min'::varchar              as period,
                   portfolio_expanded_transactions.quantity_norm::numeric *
                   chart.adjusted_close::numeric as value,
                   'realtime'                    as source
             from {{ ref('portfolio_expanded_transactions') }}
                      join {{ ref('portfolio_securities_normalized') }}
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                      join {{ ref('base_tickers') }}
                           on base_tickers.symbol = portfolio_securities_normalized.original_ticker_symbol
                      join chart_dates
                           on (chart_dates.datetime >= portfolio_expanded_transactions.datetime or
                               portfolio_expanded_transactions.datetime is null)
                      left join {{ ref('chart') }}
                                on chart.datetime <= chart_dates.datetime
                                    and chart.datetime > chart_dates.datetime - interval '1 hour'
                                    and chart.symbol = portfolio_securities_normalized.original_ticker_symbol
                                    and chart.period = '1d'
             where portfolio_expanded_transactions.type in ('buy', 'sell')
             order by portfolio_expanded_transactions.uniq_id, chart_dates.datetime, chart.datetime desc
         )

         union all

         -- options
         (
             select portfolio_expanded_transactions.profile_id,
                    time_period.datetime               as datetime,
                    time_period.period                 as period,
                    100 * portfolio_expanded_transactions.quantity_norm::numeric *
                    ticker_options.last_price::numeric as value,
                    'options'                          as source
             from {{ ref('portfolio_expanded_transactions') }}
                      join {{ ref('portfolio_securities_normalized') }}
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                      join {{ ref('ticker_options') }}
                           on ticker_options.contract_name = portfolio_securities_normalized.original_ticker_symbol
                      left join first_transaction_date on first_transaction_date.profile_id = portfolio_expanded_transactions.profile_id
                      join time_period on (first_transaction_date.profile_id is null or time_period.datetime >= first_transaction_date.date)
             where portfolio_expanded_transactions.type in ('buy', 'sell')
         )

         union all

         -- currencies
         (
             select profile_holdings_normalized.profile_id,
                    time_period.datetime                          as datetime,
                    time_period.period                            as period,
                    profile_holdings_normalized.quantity::numeric as value,
                    'currencies'                                  as source
             from {{ ref('profile_holdings_normalized') }}
                      join {{ ref('portfolio_securities_normalized') }}
                           on portfolio_securities_normalized.id = profile_holdings_normalized.security_id
                      left join first_transaction_date
                           on first_transaction_date.profile_id = profile_holdings_normalized.profile_id
                      join time_period on (first_transaction_date.profile_id is null or
                                           time_period.datetime >= first_transaction_date.date)
             where profile_holdings_normalized.type = 'cash'
               and portfolio_securities_normalized.ticker_symbol = 'CUR:USD'
         )
     ),
     chart_data_null as (
         select distinct profile_id, period, datetime from chart_data where value is null
     ),
     chart_data_not_null as (
         select profile_id, period, datetime, count(distinct source) as cnt
         from chart_data
         where value is not null
         group by profile_id, period, datetime
     ),
     chart_data_not_null_global as (
         select profile_id, count(distinct source) as cnt
         from chart_data
         where value is not null
         group by profile_id
     )

select chart_data.profile_id,
       chart_data.period,
       chart_data.datetime,
       chart_data.datetime                                                                        as date,
       (chart_data.profile_id || '_' || chart_data.datetime || '_' || chart_data.period)::varchar as id,
       sum(value)::double precision                                                               as value,
       now()                                                                                      as updated_at
from chart_data
         left join chart_data_null
                   on chart_data_null.profile_id = chart_data.profile_id
                       and chart_data_null.period = chart_data.period
                       and chart_data_null.datetime = chart_data.datetime
         join chart_data_not_null
              on chart_data_not_null.profile_id = chart_data.profile_id
                  and chart_data_not_null.period = chart_data.period
                  and chart_data_not_null.datetime = chart_data.datetime
         join chart_data_not_null_global
              on chart_data_not_null_global.profile_id = chart_data.profile_id
where chart_data_null.profile_id is null
  and chart_data_not_null.cnt = chart_data_not_null_global.cnt - 1
group by chart_data.profile_id, chart_data.period, chart_data.datetime
