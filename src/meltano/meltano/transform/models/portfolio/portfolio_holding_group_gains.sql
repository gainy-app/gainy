{{
  config(
    materialized = "view",
  )
}}


with holding_groups0 as
         (
             select holding_group_id,
                    max(updated_at)                   as updated_at,
                    sum(actual_value::numeric)        as actual_value,
                    sum(ltt_quantity_total::numeric)  as ltt_quantity_total,
                    sum(absolute_gain_1d::numeric)    as absolute_gain_1d,
                    sum(absolute_gain_1w::numeric)    as absolute_gain_1w,
                    sum(absolute_gain_1m::numeric)    as absolute_gain_1m,
                    sum(absolute_gain_3m::numeric)    as absolute_gain_3m,
                    sum(absolute_gain_1y::numeric)    as absolute_gain_1y,
                    sum(absolute_gain_5y::numeric)    as absolute_gain_5y,
                    sum(absolute_gain_total::numeric) as absolute_gain_total
             from {{ ref('portfolio_holding_gains') }}
             where portfolio_holding_gains.holding_group_id is not null
             group by portfolio_holding_gains.holding_group_id
         ),
     holding_groups as
         (
             -- HP = EV / (BV + CF) - 1
             select holding_group_id,
                    updated_at::timestamp,
                    actual_value::double precision,
                    ltt_quantity_total::double precision,
                    case
                        when abs(actual_value - absolute_gain_1d) > 0
                            then actual_value / (actual_value - absolute_gain_1d) - 1
                        end::double precision as relative_gain_1d,
                    case
                        when abs(actual_value - absolute_gain_1w) > 0
                            then actual_value / (actual_value - absolute_gain_1w) - 1
                        end::double precision as relative_gain_1w,
                    case
                        when abs(actual_value - absolute_gain_1m) > 0
                            then actual_value / (actual_value - absolute_gain_1m) - 1
                        end::double precision as relative_gain_1m,
                    case
                        when abs(actual_value - absolute_gain_3m) > 0
                            then actual_value / (actual_value - absolute_gain_3m) - 1
                        end::double precision as relative_gain_3m,
                    case
                        when abs(actual_value - absolute_gain_1y) > 0
                            then actual_value / (actual_value - absolute_gain_1y) - 1
                        end::double precision as relative_gain_1y,
                    case
                        when abs(actual_value - absolute_gain_5y) > 0
                            then actual_value / (actual_value - absolute_gain_5y) - 1
                        end::double precision as relative_gain_5y,
                    case
                        when abs(actual_value - absolute_gain_total) > 0
                            then actual_value / (actual_value - absolute_gain_total) - 1
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
