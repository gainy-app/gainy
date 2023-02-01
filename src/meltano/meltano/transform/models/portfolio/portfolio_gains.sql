{{
  config(
    materialized = "incremental",
    unique_key = "profile_id",
    tags = ["realtime"],
    post_hook=[
      index('profile_id', true),
    ]
  )
}}

with expanded_holdings as
         (
             select profile_holdings_normalized_all.profile_id,
                    max(portfolio_holding_gains.updated_at)                         as updated_at,
                    sum(actual_value::numeric)                                      as actual_value,
                    sum(absolute_gain_1d::numeric)    as absolute_gain_1d,
                    sum(absolute_gain_1w::numeric)    as absolute_gain_1w,
                    sum(absolute_gain_1m::numeric)    as absolute_gain_1m,
                    sum(absolute_gain_3m::numeric)    as absolute_gain_3m,
                    sum(absolute_gain_1y::numeric)    as absolute_gain_1y,
                    sum(absolute_gain_5y::numeric)    as absolute_gain_5y,
                    sum(absolute_gain_total::numeric) as absolute_gain_total
             from {{ ref('portfolio_holding_gains') }}
                      join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
             where not profile_holdings_normalized_all.is_hidden
             group by profile_holdings_normalized_all.profile_id
         )
select profile_id,
       updated_at,
       (actual_value + buying_power + pending_orders_sum)::double precision           as actual_value,
       (absolute_gain_1d / (actual_value - absolute_gain_1d))::double precision       as relative_gain_1d,
       (absolute_gain_1w / (actual_value - absolute_gain_1w))::double precision       as relative_gain_1w,
       (absolute_gain_1m / (actual_value - absolute_gain_1m))::double precision       as relative_gain_1m,
       (absolute_gain_3m / (actual_value - absolute_gain_3m))::double precision       as relative_gain_3m,
       (absolute_gain_1y / (actual_value - absolute_gain_1y))::double precision       as relative_gain_1y,
       (absolute_gain_5y / (actual_value - absolute_gain_5y))::double precision       as relative_gain_5y,
       (absolute_gain_total / (actual_value - absolute_gain_total))::double precision as relative_gain_total,
       absolute_gain_1d::double precision,
       absolute_gain_1w::double precision,
       absolute_gain_1m::double precision,
       absolute_gain_3m::double precision,
       absolute_gain_1y::double precision,
       absolute_gain_5y::double precision,
       absolute_gain_total::double precision
from expanded_holdings
         left join {{ ref('trading_profile_status') }} using (profile_id)
