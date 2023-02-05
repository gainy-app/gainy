{{
  config(
    materialized = "view",
  )
}}


with holding_groups as
         (
             select holding_group_id,
                    max(updated_at)                                     as updated_at,
                    sum(actual_value::numeric)                          as actual_value,
                    sum(ltt_quantity_total::numeric)                    as ltt_quantity_total,
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
                    sum(value_to_portfolio_value * relative_gain_total) as relative_gain_total,
                    sum(value_to_portfolio_value)                       as value_to_portfolio_value
             from {{ ref('portfolio_holding_gains') }}
             where portfolio_holding_gains.holding_group_id is not null
             group by portfolio_holding_gains.holding_group_id
     )
select holding_groups.*,
       profile_id,
       symbol as ticker_symbol,
       collection_id
from holding_groups
         join {{ ref('profile_holding_groups') }} on profile_holding_groups.id = holding_group_id
