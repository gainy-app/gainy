{{
  config(
    materialized = "incremental",
    unique_key = "holding_group_id",
    tags = ["realtime"],
    post_hook=[
      pk('holding_group_id'),
      'create unique index if not exists "profile_id__ticker_symbol" ON {{ this }} (profile_id, ticker_symbol)',
      'create unique index if not exists "profile_id__collection_id" ON {{ this }} (profile_id, collection_id)',
    ]
  )
}}


with ticker_holding_groups as
         (
             select profile_holdings_normalized.holding_group_id,
                    profile_holdings_normalized.profile_id,
                    ticker_symbol                     as symbol,
                    max(updated_at)                   as updated_at,
                    sum(actual_value::numeric)        as actual_value,
                    sum(absolute_gain_total::numeric) as absolute_gain_total,
                    sum(ltt_quantity_total::numeric)  as ltt_quantity_total
             from {{ ref('portfolio_holding_gains') }}
                      join {{ ref('profile_holdings_normalized') }} using (holding_id)
             where collection_id is null
               and profile_holdings_normalized.holding_group_id is not null
             group by profile_holdings_normalized.holding_group_id, profile_holdings_normalized.profile_id, symbol
         ),
     ticker_holding_groups_with_gains as
         (
             select ticker_holding_groups.holding_group_id,
                    ticker_holding_groups.profile_id,
                    ticker_holding_groups.symbol,
                    ticker_holding_groups.updated_at,
                    ticker_holding_groups.actual_value,
                    ticker_holding_groups.absolute_gain_total,
                    ticker_realtime_metrics.actual_price,
                    ticker_realtime_metrics.relative_daily_change as relative_gain_1d,
                    ticker_metrics.price_change_1w                as relative_gain_1w,
                    ticker_metrics.price_change_1m                as relative_gain_1m,
                    ticker_metrics.price_change_3m                as relative_gain_3m,
                    ticker_metrics.price_change_1y                as relative_gain_1y,
                    ticker_metrics.price_change_5y                as relative_gain_5y,
                    ticker_metrics.price_change_all               as relative_gain_total,
                    ltt_quantity_total
             from ticker_holding_groups
                      left join {{ ref('ticker_metrics') }} using (symbol)
                      left join {{ ref('ticker_realtime_metrics') }} using (symbol)
     ),
     collection_holding_groups as
         (
             select holding_group_id,
                    profile_id,
                    collection_id,
                    greatest(max(drivewealth_holdings.updated_at),
                             max(drivewealth_holdings_gains.updated_at)) as updated_at,
                    sum(drivewealth_holdings.actual_value::numeric)      as actual_value,
                    sum(ltt_quantity_total::numeric)                     as ltt_quantity_total,
                    sum(drivewealth_holdings_gains.absolute_gain_1d)     as absolute_gain_1d,
                    sum(drivewealth_holdings_gains.absolute_gain_1w)     as absolute_gain_1w,
                    sum(drivewealth_holdings_gains.absolute_gain_1m)     as absolute_gain_1m,
                    sum(drivewealth_holdings_gains.absolute_gain_3m)     as absolute_gain_3m,
                    sum(drivewealth_holdings_gains.absolute_gain_1y)     as absolute_gain_1y,
                    sum(drivewealth_holdings_gains.absolute_gain_5y)     as absolute_gain_5y,
                    sum(drivewealth_holdings_gains.absolute_gain_total)  as absolute_gain_total
             from {{ ref('drivewealth_holdings') }}
                      join {{ ref('drivewealth_holdings_gains') }} using (profile_id, collection_id, symbol)
             where collection_id is not null
             group by holding_group_id, profile_id, collection_id
     ),
     combined_holding_groups as
         (
             select holding_group_id,
                    profile_id,
                    symbol                                                                 as ticker_symbol,
                    null                                                                   as collection_id,
                    updated_at,
                    actual_value::double precision,
                    ltt_quantity_total::double precision,
                    relative_gain_1d::double precision,
                    relative_gain_1w::double precision,
                    relative_gain_1m::double precision,
                    relative_gain_3m::double precision,
                    relative_gain_1y::double precision,
                    relative_gain_5y::double precision,
                    relative_gain_total::double precision,
                    (actual_price * (1 - 1 / (1 + relative_gain_1d)))::double precision    as absolute_gain_1d,
                    (actual_price * (1 - 1 / (1 + relative_gain_1w)))::double precision    as absolute_gain_1w,
                    (actual_price * (1 - 1 / (1 + relative_gain_1m)))::double precision    as absolute_gain_1m,
                    (actual_price * (1 - 1 / (1 + relative_gain_3m)))::double precision    as absolute_gain_3m,
                    (actual_price * (1 - 1 / (1 + relative_gain_1y)))::double precision    as absolute_gain_1y,
                    (actual_price * (1 - 1 / (1 + relative_gain_5y)))::double precision    as absolute_gain_5y,
                    (actual_price * (1 - 1 / (1 + relative_gain_total)))::double precision as absolute_gain_total
             from ticker_holding_groups_with_gains

             union all

             select holding_group_id,
                    profile_id,
                    null                                                                           as ticker_symbol,
                    collection_id,
                    updated_at,
                    actual_value::double precision,
                    ltt_quantity_total,
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
             from collection_holding_groups
     )

select *,
       (actual_value / (1e-9 + sum(actual_value) over (partition by profile_id)))::double precision as value_to_portfolio_value
from combined_holding_groups
