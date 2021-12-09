{{
  config(
    materialized = "table",
    post_hook=[
      'create unique index if not exists {{ get_index_name(this, "profile_id__ticker_symbol") }} (profile_id, ticker_symbol)',
    ]
  )
}}

with expanded_holding_groups as
         (
             select profile_id,
                    ticker_symbol,
                    max(updated_at)                        as updated_at,
                    sum(actual_value::numeric)             as actual_value,
                    sum(value_to_portfolio_value::numeric) as value_to_portfolio_value,
                    sum(absolute_gain_1d::numeric)         as absolute_gain_1d,
                    sum(absolute_gain_1w::numeric)         as absolute_gain_1w,
                    sum(absolute_gain_1m::numeric)         as absolute_gain_1m,
                    sum(absolute_gain_3m::numeric)         as absolute_gain_3m,
                    sum(absolute_gain_1y::numeric)         as absolute_gain_1y,
                    sum(absolute_gain_5y::numeric)         as absolute_gain_5y,
                    sum(absolute_gain_total::numeric)      as absolute_gain_total,
                    sum(ltt_quantity_total::numeric)       as ltt_quantity_total
             from {{ ref('portfolio_holding_gains') }}
                      join {{ ref('profile_holdings_normalized') }}
                           on profile_holdings_normalized.holding_id = portfolio_holding_gains.holding_id
             group by profile_id, ticker_symbol
         )
select profile_id,
       ticker_symbol,
       updated_at,
       actual_value::double precision,
       value_to_portfolio_value::double precision,
       ltt_quantity_total::double precision,
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
from expanded_holding_groups