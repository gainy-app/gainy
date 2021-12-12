{{
  config(
    materialized = "incremental",
    unique_key = "id",
    incremental_strategy = 'insert_overwrite',
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "profile_id__date__period") }} (profile_id, date, period)',
    ]
  )
}}


{% if is_incremental() %}
with max_date as
         (
             select profile_id,
                    period,
                    max(date) as date
             from {{ this }} old_portfolio_chart
             group by profile_id, period
         )
{% endif %}
select profile_id,
       period,
       datetime,
       datetime                                                  as date, -- TODO remove
       (profile_id || '_' || datetime || '_' || period)::varchar as id,
       sum(value)::double precision                              as value
from (
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
{% if is_incremental() %}
                      left join max_date
                                on max_date.profile_id = portfolio_expanded_transactions.profile_id and
                                   max_date.period = historical_prices_aggregated.period
{% endif %}
             where portfolio_expanded_transactions.type in ('buy', 'sell')
{% if is_incremental() %}
               and (max_date.date is null or historical_prices_aggregated.time >= max_date.date)
{% endif %}
         )

         union all

         -- options
         (
             with time_period as
                      (
                          select distinct datetime, period
                          from historical_prices_aggregated
                      ),
                  first_transaction_date as
                      (
                          select profile_id,
                                 min(date) as date
                          from {{ source('app', 'profile_portfolio_transactions') }}
                          group by profile_id
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
                      join time_period on time_period.datetime >= first_transaction_date.date
{% if is_incremental() %}
                      left join max_date
                                on max_date.profile_id = portfolio_expanded_transactions.profile_id and
                                   max_date.period = time_period.period
{% endif %}
             where portfolio_expanded_transactions.type in ('buy', 'sell')
{% if is_incremental() %}
               and (max_date.date is null or time_period.datetime >= max_date.date)
{% endif %}
         )
     ) t
group by profile_id, period, datetime
