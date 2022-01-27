{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "profile_id__date__period") }} (profile_id, date, period)',
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
     current_transaction_stats as
         (
             select profile_id,
                    count(portfolio_expanded_transactions.uniq_id) as transactions_count,
                    max(portfolio_expanded_transactions.datetime)  as last_transaction_datetime
             from portfolio_expanded_transactions
                      join portfolio_securities_normalized
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
             where portfolio_expanded_transactions.type in ('buy', 'sell')
             group by profile_id
{% if is_incremental() %}
         ),
     old_transaction_stats as
         (
             select profile_id,
                    max(transactions_count)        as transactions_count,
                    max(last_transaction_datetime) as last_transaction_datetime
             from {{ this }}
             group by profile_id
         ),
     max_date as
         (
             select profile_id,
                    period,
                    max(date) as date
             from {{ this }}
             group by profile_id, period
{% endif %}
         ),
     chart_data as (
         -- stocks
         (
             select portfolio_expanded_transactions.profile_id,
                    historical_prices_aggregated.time                    as datetime,
                    historical_prices_aggregated.period                  as period,
                    portfolio_expanded_transactions.quantity_norm::numeric *
                    historical_prices_aggregated.adjusted_close::numeric as value
             from {{ ref('portfolio_expanded_transactions') }}
                      join {{ ref('portfolio_securities_normalized') }}
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                      join {{ ref('historical_prices_aggregated') }}
                           on (historical_prices_aggregated.datetime >= portfolio_expanded_transactions.datetime or
                               portfolio_expanded_transactions.datetime is null) and
                              historical_prices_aggregated.symbol = portfolio_securities_normalized.ticker_symbol
                      left join first_transaction_date on first_transaction_date.profile_id = portfolio_expanded_transactions.profile_id
                      join current_transaction_stats on current_transaction_stats.profile_id = portfolio_expanded_transactions.profile_id
{% if is_incremental() %}
                      left join old_transaction_stats on old_transaction_stats.profile_id = portfolio_expanded_transactions.profile_id
                      left join max_date
                                on max_date.profile_id = portfolio_expanded_transactions.profile_id and
                                   max_date.period = historical_prices_aggregated.period
{% endif %}
             where portfolio_expanded_transactions.type in ('buy', 'sell')
               and (first_transaction_date.profile_id is null or historical_prices_aggregated.time >= first_transaction_date.date)
               and historical_prices_aggregated.period != '15min'
{% if is_incremental() %}
               and (old_transaction_stats.profile_id is null
                 or old_transaction_stats.transactions_count != current_transaction_stats.transactions_count
                 or old_transaction_stats.last_transaction_datetime != current_transaction_stats.last_transaction_datetime
                 or max_date.date is null
                 or historical_prices_aggregated.time >= max_date.date)
{% endif %}
         )

         union all

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
                   chart.adjusted_close::numeric as value
             from {{ ref('portfolio_expanded_transactions') }}
                      join {{ ref('portfolio_securities_normalized') }}
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                      join {{ ref('base_tickers') }}
                           on base_tickers.symbol = portfolio_securities_normalized.ticker_symbol
                      join chart_dates
                           on (chart_dates.datetime >= portfolio_expanded_transactions.datetime or
                               portfolio_expanded_transactions.datetime is null)
                      left join {{ ref('chart') }}
                                on chart.datetime <= chart_dates.datetime
                                    and chart.datetime > chart_dates.datetime - interval '1 hour'
                                    and chart.symbol = portfolio_securities_normalized.ticker_symbol
                                    and chart.period = '1d'
{% if is_incremental() %}
                      join current_transaction_stats
                           on current_transaction_stats.profile_id = portfolio_expanded_transactions.profile_id
                      left join old_transaction_stats on old_transaction_stats.profile_id = portfolio_expanded_transactions.profile_id
                      left join max_date
                                on max_date.profile_id = portfolio_expanded_transactions.profile_id and
                                   max_date.period = '15min'
{% endif %}
             where portfolio_expanded_transactions.type in ('buy', 'sell')

{% if is_incremental() %}
               and (old_transaction_stats.profile_id is null
                 or old_transaction_stats.transactions_count != current_transaction_stats.transactions_count
                 or old_transaction_stats.last_transaction_datetime != current_transaction_stats.last_transaction_datetime
                 or max_date.date is null
                 or chart_dates.datetime is null
                 or chart_dates.datetime >= max_date.date)
             order by portfolio_expanded_transactions.uniq_id, chart_dates.datetime, chart.datetime desc
{% endif %}
         )

         union all

         -- options
         (
             with time_period as
                      (
                          select distinct datetime, period
                          from {{ ref('historical_prices_aggregated') }}
                          where historical_prices_aggregated.period != '15min'
                          union all
                          select distinct datetime, '15min'::varchar as period
                          from {{ ref('chart') }}
                          where chart.period = '1d'
                      )
             select portfolio_expanded_transactions.profile_id,
                    time_period.datetime               as datetime,
                    time_period.period                 as period,
                    100 * portfolio_expanded_transactions.quantity_norm::numeric *
                    ticker_options.last_price::numeric as value
             from {{ ref('portfolio_expanded_transactions') }}
                      join {{ ref('portfolio_securities_normalized') }}
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                      join {{ ref('ticker_options') }}
                           on ticker_options.contract_name = portfolio_securities_normalized.original_ticker_symbol
                      join first_transaction_date on first_transaction_date.profile_id = portfolio_expanded_transactions.profile_id
                      join time_period on (first_transaction_date.profile_id is null or time_period.datetime >= first_transaction_date.date)
                      join current_transaction_stats on current_transaction_stats.profile_id = portfolio_expanded_transactions.profile_id
{% if is_incremental() %}
                      left join old_transaction_stats on old_transaction_stats.profile_id = portfolio_expanded_transactions.profile_id
                      left join max_date
                                on max_date.profile_id = portfolio_expanded_transactions.profile_id and
                                   max_date.period = time_period.period
{% endif %}
             where portfolio_expanded_transactions.type in ('buy', 'sell')
{% if is_incremental() %}
               and (old_transaction_stats.profile_id is null
                 or old_transaction_stats.transactions_count != current_transaction_stats.transactions_count
                 or old_transaction_stats.last_transaction_datetime != current_transaction_stats.last_transaction_datetime
                 or max_date.date is null
                 or time_period.datetime >= max_date.date)
{% endif %}
         )
     ),
     chart_data_null as (
         select distinct profile_id, period, datetime from chart_data where value is null
     )

select chart_data.profile_id,
       chart_data.period,
       chart_data.datetime,
       chart_data.datetime                                                                        as date,
       (chart_data.profile_id || '_' || chart_data.datetime || '_' || chart_data.period)::varchar as id,
       sum(value)::double precision                                                               as value,
       max(current_transaction_stats.transactions_count)                                          as transactions_count,
       max(current_transaction_stats.last_transaction_datetime)                                   as last_transaction_datetime
from chart_data
         join current_transaction_stats on current_transaction_stats.profile_id = chart_data.profile_id
         left join chart_data_null
                   on chart_data_null.profile_id = chart_data.profile_id
                       and chart_data_null.period = chart_data.period
                       and chart_data_null.datetime = chart_data.datetime
where chart_data_null.profile_id is null
group by chart_data.profile_id, chart_data.period, chart_data.datetime
