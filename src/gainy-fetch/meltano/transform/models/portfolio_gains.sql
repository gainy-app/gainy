{{
  config(
    materialized = "incremental",
    unique_key = "profile_id",
    incremental_strategy = 'insert_overwrite',
    post_hook=[
      index(this, 'profile_id', true),
    ]
  )
}}

with expanded_holdings as
         (
             select t.profile_id,
                    min(updated_at)          as updated_at,
                    sum(actual_value)        as actual_value,
                    sum(absolute_gain_1w)    as absolute_gain_1w,
                    sum(absolute_gain_1m)    as absolute_gain_1m,
                    sum(absolute_gain_3m)    as absolute_gain_3m,
                    sum(absolute_gain_1y)    as absolute_gain_1y,
                    sum(absolute_gain_5y)    as absolute_gain_5y,
                    sum(absolute_gain_total) as absolute_gain_total
             from (
                      select distinct on (
                          portfolio_holding_gains.holding_id
                          ) profile_holdings.profile_id,
                            portfolio_holding_gains.updated_at,
                            portfolio_holding_gains.actual_value::numeric,
                            portfolio_holding_gains.absolute_gain_1w::numeric,
                            portfolio_holding_gains.absolute_gain_1m::numeric,
                            portfolio_holding_gains.absolute_gain_3m::numeric,
                            portfolio_holding_gains.absolute_gain_1y::numeric,
                            portfolio_holding_gains.absolute_gain_5y::numeric,
                            portfolio_holding_gains.absolute_gain_total::numeric
                      from {{ ref('portfolio_holding_gains') }}
                               join {{ source ('app', 'profile_holdings') }} on profile_holdings.id = portfolio_holding_gains.holding_id
                               join {{ source ('app', 'portfolio_securities') }}
                                    on portfolio_securities.id = profile_holdings.security_id
                               join {{ ref('historical_prices') }}
                                    on historical_prices.code = portfolio_securities.ticker_symbol
                      {% if is_incremental() %}
                               left join {{ this }} old_portfolio_gains
                                    on old_portfolio_gains.profile_id = profile_holdings.profile_id
                      where old_portfolio_gains.updated_at is null or portfolio_holding_gains.updated_at > old_portfolio_gains.updated_at
                      {% endif %}
                      order by portfolio_holding_gains.holding_id, historical_prices.date desc
                  ) t
             group by t.profile_id
         )
select profile_id,
       updated_at,
       actual_value::double precision,
       (absolute_gain_1w / (actual_value - absolute_gain_1w))::double precision       as relative_gain_1w,
       (absolute_gain_1m / (actual_value - absolute_gain_1m))::double precision       as relative_gain_1m,
       (absolute_gain_3m / (actual_value - absolute_gain_3m))::double precision       as relative_gain_3m,
       (absolute_gain_1y / (actual_value - absolute_gain_1y))::double precision       as relative_gain_1y,
       (absolute_gain_5y / (actual_value - absolute_gain_5y))::double precision       as relative_gain_5y,
       (absolute_gain_total / (actual_value - absolute_gain_total))::double precision as relative_gain_total,
       absolute_gain_1w::double precision,
       absolute_gain_1m::double precision,
       absolute_gain_3m::double precision,
       absolute_gain_1y::double precision,
       absolute_gain_5y::double precision,
       absolute_gain_total::double precision
from expanded_holdings