{{
  config(
    materialized = "view",
  )
}}

with expanded_holdings0 as
         (
             select profile_id,
                    max(plaid_holding_gains.updated_at)                         as updated_at,
                    sum(actual_value)                                           as actual_value,
                    sum(absolute_gain_1d)                                       as absolute_gain_1d,
                    sum(absolute_gain_1w)                                       as absolute_gain_1w,
                    sum(absolute_gain_1m)                                       as absolute_gain_1m,
                    sum(absolute_gain_3m)                                       as absolute_gain_3m,
                    sum(absolute_gain_1y)                                       as absolute_gain_1y,
                    sum(absolute_gain_5y)                                       as absolute_gain_5y,
                    sum(absolute_gain_total)                                    as absolute_gain_total,
                    sum(actual_value * relative_gain_1d) / sum(actual_value)    as relative_gain_1d,
                    sum(actual_value * relative_gain_1w) / sum(actual_value)    as relative_gain_1w,
                    sum(actual_value * relative_gain_1m) / sum(actual_value)    as relative_gain_1m,
                    sum(actual_value * relative_gain_3m) / sum(actual_value)    as relative_gain_3m,
                    sum(actual_value * relative_gain_1y) / sum(actual_value)    as relative_gain_1y,
                    sum(actual_value * relative_gain_5y) / sum(actual_value)    as relative_gain_5y,
                    sum(actual_value * relative_gain_total) / sum(actual_value) as relative_gain_total
             from {{ ref('plaid_holding_gains') }}
                      join {{ ref('profile_holdings_normalized_all') }} using (profile_id, holding_id_v2)
             group by profile_id

             union all

             select profile_id,
                    updated_at,
                    actual_value,
                    absolute_gain_1d,
                    absolute_gain_1w,
                    absolute_gain_1m,
                    absolute_gain_3m,
                    absolute_gain_1y,
                    absolute_gain_5y,
                    absolute_gain_total,
                    relative_gain_1d,
                    relative_gain_1w,
                    relative_gain_1m,
                    relative_gain_3m,
                    relative_gain_1y,
                    relative_gain_5y,
                    relative_gain_total
             from {{ ref('drivewealth_portfolio_gains') }}
     ),
     expanded_holdings1 as
         (
             select *,
                    (actual_value / (1e-9 + sum(actual_value) over (partition by profile_id)))::double precision as value_to_portfolio_value
             from expanded_holdings0
     ),
     expanded_holdings as
         (
             select profile_id,
                    max(updated_at)                                     as updated_at,
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
             from expanded_holdings1
             group by profile_id
     )
select profile_id,
       greatest(
           expanded_holdings.updated_at,
           trading_profile_status.updated_at
           )::timestamp                                             as updated_at,
       (actual_value + coalesce(pending_cash, 0))::double precision as actual_value,
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
