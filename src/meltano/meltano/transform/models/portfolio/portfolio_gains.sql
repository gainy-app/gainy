{{
  config(
    materialized = "view",
  )
}}

with expanded_holdings0 as
         (
             select profile_id,
                    max(portfolio_holding_gains.updated_at)                                   as updated_at,
                    sum(actual_value)                                                         as actual_value,
                    sum(case when is_app_trading = false then absolute_gain_1d else 0 end)    as absolute_gain_1d,
                    sum(case when is_app_trading = false then absolute_gain_1w else 0 end)    as absolute_gain_1w,
                    sum(case when is_app_trading = false then absolute_gain_1m else 0 end)    as absolute_gain_1m,
                    sum(case when is_app_trading = false then absolute_gain_3m else 0 end)    as absolute_gain_3m,
                    sum(case when is_app_trading = false then absolute_gain_1y else 0 end)    as absolute_gain_1y,
                    sum(case when is_app_trading = false then absolute_gain_5y else 0 end)    as absolute_gain_5y,
                    sum(case when is_app_trading = false then absolute_gain_total else 0 end) as absolute_gain_total,
                    sum(value_to_portfolio_value * relative_gain_1d)                          as relative_gain_1d,
                    sum(value_to_portfolio_value * relative_gain_1w)                          as relative_gain_1w,
                    sum(value_to_portfolio_value * relative_gain_1m)                          as relative_gain_1m,
                    sum(value_to_portfolio_value * relative_gain_3m)                          as relative_gain_3m,
                    sum(value_to_portfolio_value * relative_gain_1y)                          as relative_gain_1y,
                    sum(value_to_portfolio_value * relative_gain_5y)                          as relative_gain_5y,
                    sum(value_to_portfolio_value * relative_gain_total)                       as relative_gain_total
             from {{ ref('portfolio_holding_gains') }}
                      join {{ ref('profile_holdings_normalized_all') }} using (profile_id, holding_id_v2)
             group by profile_id
     ),
     expanded_holdings as
         (
             select profile_id,
                    greatest(
                            expanded_holdings0.updated_at,
                            drivewealth_portfolio_gains.updated_at
                        )::timestamp                                             as updated_at,
                    actual_value,
                    coalesce(expanded_holdings0.absolute_gain_1d, 0) +
                    coalesce(drivewealth_portfolio_gains.absolute_gain_1d, 0)    as absolute_gain_1d,
                    coalesce(expanded_holdings0.absolute_gain_1w, 0) +
                    coalesce(drivewealth_portfolio_gains.absolute_gain_1w, 0)    as absolute_gain_1w,
                    coalesce(expanded_holdings0.absolute_gain_1m, 0) +
                    coalesce(drivewealth_portfolio_gains.absolute_gain_1m, 0)    as absolute_gain_1m,
                    coalesce(expanded_holdings0.absolute_gain_3m, 0) +
                    coalesce(drivewealth_portfolio_gains.absolute_gain_3m, 0)    as absolute_gain_3m,
                    coalesce(expanded_holdings0.absolute_gain_1y, 0) +
                    coalesce(drivewealth_portfolio_gains.absolute_gain_1y, 0)    as absolute_gain_1y,
                    coalesce(expanded_holdings0.absolute_gain_5y, 0) +
                    coalesce(drivewealth_portfolio_gains.absolute_gain_5y, 0)    as absolute_gain_5y,
                    coalesce(expanded_holdings0.absolute_gain_total, 0) +
                    coalesce(drivewealth_portfolio_gains.absolute_gain_total, 0) as absolute_gain_total,
                    relative_gain_1d,
                    relative_gain_1w,
                    relative_gain_1m,
                    relative_gain_3m,
                    relative_gain_1y,
                    relative_gain_5y,
                    relative_gain_total
             from (
                      select id as profile_id
                      from {{ source('app', 'profiles') }}
                  ) t
                      left join expanded_holdings0 using (profile_id)
                      left join {{ ref('drivewealth_portfolio_gains') }} using (profile_id)
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
