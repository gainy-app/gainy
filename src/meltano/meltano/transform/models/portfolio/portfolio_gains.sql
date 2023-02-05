{{
  config(
    materialized = "view",
  )
}}

with expanded_holdings as
         (
             select profile_id,
                    max(portfolio_holding_gains.updated_at)             as updated_at,
                    sum(actual_value)                                   as actual_value,
                    sum(absolute_gain_1d)                               as absolute_gain_1d,
                    sum(absolute_gain_1w)                               as absolute_gain_1w,
                    sum(absolute_gain_1m)                               as absolute_gain_1m,
                    sum(absolute_gain_3m)                               as absolute_gain_3m,
                    sum(absolute_gain_1y)                               as absolute_gain_1y,
                    sum(absolute_gain_5y)                               as absolute_gain_5y,
                    sum(absolute_gain_total)                            as absolute_gain_total,
                    sum(value_to_portfolio_value * relative_gain_1d)    as relative_gain_1d,
                    sum(value_to_portfolio_value * relative_gain_1w)    as relative_gain_1w,
                    sum(value_to_portfolio_value * relative_gain_1m)    as relative_gain_1m,
                    sum(value_to_portfolio_value * relative_gain_3m)    as relative_gain_3m,
                    sum(value_to_portfolio_value * relative_gain_1y)    as relative_gain_1y,
                    sum(value_to_portfolio_value * relative_gain_5y)    as relative_gain_5y,
                    sum(value_to_portfolio_value * relative_gain_total) as relative_gain_total
             from {{ ref('portfolio_holding_gains') }}
             group by profile_id
         )
select profile_id,
       greatest(
           expanded_holdings.updated_at,
           trading_profile_status.updated_at
           )::timestamp                                    as updated_at,
       (actual_value + coalesce(buying_power, 0) +
        coalesce(pending_orders_sum, 0))::double precision as actual_value,
       relative_gain_1d::double precision,
       relative_gain_1w::double precision,
       relative_gain_1m::double precision,
       relative_gain_3m::double precision,
       relative_gain_1y::double precision,
       relative_gain_5y::double precision,
       relative_gain_total::double precision,
       absolute_gain_1d::double precision,
       absolute_gain_1w::double precision,
       absolute_gain_1m::double precision,
       absolute_gain_3m::double precision,
       absolute_gain_1y::double precision,
       absolute_gain_5y::double precision,
       absolute_gain_total::double precision
from expanded_holdings
         left join {{ ref('trading_profile_status') }} using (profile_id)
