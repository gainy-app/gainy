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


with
{% if is_incremental() %}
     max_date as
         (
             select profile_id,
                    max(date) as date
             from portfolio_chart old_portfolio_chart
             group by profile_id
      ),
{% endif %}
     daily_data as
         (
             select portfolio_securities.profile_id,
                    historical_prices.date                         as date,
                    sum(profile_portfolio_transactions.quantity::numeric *
                        historical_prices.adjusted_close::numeric) as value
             from {{ source('app', 'profile_portfolio_transactions') }}
                      join {{ source('app', 'portfolio_securities') }}
                           on portfolio_securities.id = profile_portfolio_transactions.security_id
             {% if is_incremental() %}
                      left join max_date on max_date.profile_id = portfolio_securities.profile_id
             {% endif %}
                      join {{ ref('historical_prices') }}
                           on historical_prices.date >= profile_portfolio_transactions.date and
             {% if is_incremental() %}
                              (max_date.date is null or historical_prices.date >= max_date.date) and
             {% endif %}
                              historical_prices.code = portfolio_securities.ticker_symbol
             where profile_portfolio_transactions.type in ('buy', 'sell')
               and portfolio_securities.type in ('mutual fund', 'equity', 'etf')
             group by portfolio_securities.profile_id, historical_prices.date
         )

select (profile_id || '_' || date|| '_1d')::varchar as id,
       profile_id,
       date::timestamp                      as date,
       '1d'::varchar                        as period,
       value::double precision
from daily_data

union

(
    select DISTINCT ON (
        profile_id,
        date_part('year', date),
        date_part('week', date)
        ) (profile_id || '_' || date || '_1w')::varchar                                                       as id,
          profile_id,
          date_trunc('week', date)::timestamp                                                                 as date,
          '1w'::varchar                                                                                       as period,
          first_value(value::double precision)
          OVER (partition by profile_id, date_part('year', date), date_part('week', date) order by date desc) as value
    from daily_data
    order by profile_id, date_part('year', date), date_part('week', date), date
)

union

(
    select DISTINCT ON (
        profile_id,
        date_part('year', date),
        date_part('month', date)
        ) (profile_id || '_' || date || '_1m')::varchar                                                        as id,
          profile_id,
          date_trunc('month', date)::timestamp                                                                 as date,
          '1m'::varchar                                                                                        as period,
          first_value(value::double precision)
          OVER (partition by profile_id, date_part('year', date), date_part('month', date) order by date desc) as value
    from daily_data
    order by profile_id, date_part('year', date), date_part('month', date), date
)