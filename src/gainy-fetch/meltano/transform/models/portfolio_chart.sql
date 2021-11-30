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
                    max(date) as date
             from portfolio_chart old_portfolio_chart
             group by profile_id
      )
{% endif %}
select profile_portfolio_transactions.profile_id,
       (profile_portfolio_transactions.profile_id || '_' || historical_prices_aggregated.time || '_' ||
        historical_prices_aggregated.period)::varchar                              as id,
       historical_prices_aggregated.time                                           as date, -- TODO remove
       historical_prices_aggregated.time                                           as datetime,
       historical_prices_aggregated.period                                         as period,
       sum(profile_portfolio_transactions.quantity::numeric *
           historical_prices_aggregated.adjusted_close::numeric)::double precision as value
from {{ source('app', 'profile_portfolio_transactions') }}
         join {{ source('app', 'portfolio_securities') }}
              on portfolio_securities.id = profile_portfolio_transactions.security_id
{% if is_incremental() %}
         left join max_date on max_date.profile_id = profile_portfolio_transactions.profile_id
{% endif %}
         join {{ ref('historical_prices_aggregated') }}
              on historical_prices_aggregated.time >= profile_portfolio_transactions.date and
{% if is_incremental() %}
                 (max_date.date is null or historical_prices_aggregated.time >= max_date.date) and
{% endif %}
                 historical_prices_aggregated.symbol = portfolio_securities.ticker_symbol
where profile_portfolio_transactions.type in ('buy', 'sell')
  and portfolio_securities.type in ('mutual fund', 'equity', 'etf')
group by profile_portfolio_transactions.profile_id, historical_prices_aggregated.period, historical_prices_aggregated.time
