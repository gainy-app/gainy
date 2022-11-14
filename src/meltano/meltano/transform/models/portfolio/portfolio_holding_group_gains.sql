{{
  config(
    materialized = "incremental",
    unique_key = "holding_group_id",
    tags = ["realtime"],
    post_hook=[
      pk('holding_group_id'),
      'create index if not exists "profile_id__ticker_symbol" ON {{ this }} (profile_id, ticker_symbol)',
      'create index if not exists "profile_id__collection_id" ON {{ this }} (profile_id, collection_id)',
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


with holding_groups0 as
         (
             select profile_holdings_normalized.holding_group_id,
                    greatest(max(portfolio_holding_gains.updated_at),
                             max(profile_holdings_normalized.updated_at))     as updated_at,
                    sum(portfolio_holding_gains.actual_value::numeric)        as actual_value,
                    sum(portfolio_holding_gains.ltt_quantity_total::numeric)  as ltt_quantity_total,
                    sum(portfolio_holding_gains.absolute_gain_1d::numeric)    as absolute_gain_1d,
                    sum(portfolio_holding_gains.absolute_gain_1w::numeric)    as absolute_gain_1w,
                    sum(portfolio_holding_gains.absolute_gain_1m::numeric)    as absolute_gain_1m,
                    sum(portfolio_holding_gains.absolute_gain_3m::numeric)    as absolute_gain_3m,
                    sum(portfolio_holding_gains.absolute_gain_1y::numeric)    as absolute_gain_1y,
                    sum(portfolio_holding_gains.absolute_gain_5y::numeric)    as absolute_gain_5y,
                    sum(portfolio_holding_gains.absolute_gain_total::numeric) as absolute_gain_total
             from {{ ref('portfolio_holding_gains') }}
                      join {{ ref('profile_holdings_normalized') }} using (holding_id_v2)
             where profile_holdings_normalized.holding_group_id is not null
             group by profile_holdings_normalized.holding_group_id
         ),
     holding_groups as
         (
             select holding_group_id,
                    updated_at::timestamp,
                    actual_value::double precision,
                    ltt_quantity_total::double precision,
                    case
                        when abs(actual_value - absolute_gain_1d) > 0
                            then (absolute_gain_1d / (actual_value - absolute_gain_1d))
                        end::double precision       as relative_gain_1d,
                    case
                        when abs(actual_value - absolute_gain_1w) > 0
                            then (absolute_gain_1w / (actual_value - absolute_gain_1w))
                        end::double precision       as relative_gain_1w,
                    case
                        when abs(actual_value - absolute_gain_1m) > 0
                            then (absolute_gain_1m / (actual_value - absolute_gain_1m))
                        end::double precision       as relative_gain_1m,
                    case
                        when abs(actual_value - absolute_gain_3m) > 0
                            then (absolute_gain_3m / (actual_value - absolute_gain_3m))
                        end::double precision       as relative_gain_3m,
                    case
                        when abs(actual_value - absolute_gain_1y) > 0
                            then (absolute_gain_1y / (actual_value - absolute_gain_1y))
                        end::double precision       as relative_gain_1y,
                    case
                        when abs(actual_value - absolute_gain_5y) > 0
                            then (absolute_gain_5y / (actual_value - absolute_gain_5y))
                        end::double precision       as relative_gain_5y,
                    case
                        when abs(actual_value - absolute_gain_total) > 0
                            then (absolute_gain_total / (actual_value - absolute_gain_total))
                        end::double precision as relative_gain_total,
                    absolute_gain_1d::double precision,
                    absolute_gain_1w::double precision,
                    absolute_gain_1m::double precision,
                    absolute_gain_3m::double precision,
                    absolute_gain_1y::double precision,
                    absolute_gain_5y::double precision,
                    absolute_gain_total::double precision
             from holding_groups0
     )

select holding_groups.*,
       profile_id,
       symbol                                                                                       as ticker_symbol,
       collection_id,
       (actual_value / (1e-9 + sum(actual_value) over (partition by profile_id)))::double precision as value_to_portfolio_value
from holding_groups
         join {{ ref('profile_holding_groups') }} on profile_holding_groups.id = holding_group_id

