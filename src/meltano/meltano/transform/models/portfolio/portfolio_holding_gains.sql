{{
  config(
    materialized = "incremental",
    unique_key = "holding_id_v2",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id_v2'),
      'delete from {{this}}
        using {{this}} AS t
        LEFT OUTER JOIN {{ ref(\'profile_holdings_normalized\') }} using (holding_id_v2)
        WHERE portfolio_holding_gains.holding_id_v2 = t.holding_id_v2
          AND profile_holdings_normalized.holding_id_v2 is null',
    ]
  )
}}


with plaid_holdings as
         (

             select holding_id_v2,
                    case
                        when profile_holdings_normalized.type = 'cash' and
                             profile_holdings_normalized.symbol = 'CUR:USD'
                            then profile_holdings_normalized.quantity
                        else ticker_realtime_metrics.actual_price *
                             profile_holdings_normalized.quantity_norm_for_valuation
                        end                                       as actual_value
             from {{ ref('profile_holdings_normalized') }}
                      left join {{ ref('ticker_realtime_metrics') }} using (symbol)
         ),
     plaid_gains as
         (
             select holding_id_v2,
                    sum(case when relative_daily_change > -1
                            then quantity_norm_for_valuation * (actual_price - actual_price / (relative_daily_change + 1))
                        end) as absolute_gain_1d,
                    sum(case when relative_gain_1w > -1
                            then quantity_norm_for_valuation * (actual_price - actual_price / (relative_gain_1w + 1))
                        end) as absolute_gain_1w,
                    sum(case when relative_gain_1m > -1
                            then quantity_norm_for_valuation * (actual_price - actual_price / (relative_gain_1m + 1))
                        end) as absolute_gain_1m,
                    sum(case when relative_gain_3m > -1
                            then quantity_norm_for_valuation * (actual_price - actual_price / (relative_gain_3m + 1))
                        end) as absolute_gain_3m,
                    sum(case when relative_gain_1y > -1
                            then quantity_norm_for_valuation * (actual_price - actual_price / (relative_gain_1y + 1))
                        end) as absolute_gain_1y,
                    sum(case when relative_gain_5y > -1
                            then quantity_norm_for_valuation * (actual_price - actual_price / (relative_gain_5y + 1))
                        end) as absolute_gain_5y,
                    sum(case when relative_gain_all > -1
                            then quantity_norm_for_valuation * (actual_price - actual_price / (relative_gain_all + 1))
                        end) as absolute_gain_total
             from {{ ref('portfolio_expanded_transactions') }}
                      left join {{ ref('portfolio_transaction_metrics') }} using (transaction_uniq_id)
                      left join {{ ref('ticker_realtime_metrics') }} using (symbol)
             group by holding_id_v2
         ),
     long_term_tax_holdings as
         (
             select distinct on (
                 holding_id_v2
                 ) holding_id_v2,
                   ltt_quantity_total::double precision
             from (
                      select profile_holdings_normalized.holding_id_v2,
                             quantity_sign,
                             datetime,
                             min(cumsum) over wnd as ltt_quantity_total
                      from (
                               select portfolio_expanded_transactions.profile_id,
                                      holding_id_v2,
                                      datetime,
                                      sign(quantity_norm)                                                   as quantity_sign,
                                      sum(quantity_norm)
                                      over (partition by holding_id_v2 order by sign(quantity_norm), datetime) as cumsum
                               from {{ ref('portfolio_expanded_transactions') }}
                               where portfolio_expanded_transactions.profile_id is not null
                           ) t
                               join {{ ref('profile_holdings_normalized') }} using (holding_id_v2)
                      window wnd as (partition by t.holding_id_v2
                                     order by t.quantity_sign, datetime
                                     rows between current row and unbounded following)
                  ) t
             where datetime < now() - interval '1 year'
             order by holding_id_v2, quantity_sign desc, datetime desc
         ),
    combined_gains as
        (
             select holding_id_v2,
                    ticker_realtime_metrics.relative_daily_change                                                         as relative_gain_1d,
                    coalesce(drivewealth_portfolio_holding_gains_realtime.actual_value, plaid_holdings.actual_value)      as actual_value,
                    coalesce(drivewealth_portfolio_holding_gains_realtime.absolute_gain_1d, plaid_gains.absolute_gain_1d) as absolute_gain_1d,
                    coalesce(drivewealth_portfolio_holding_gains_realtime.absolute_gain_1w, plaid_gains.absolute_gain_1w) as absolute_gain_1w,
                    coalesce(drivewealth_portfolio_holding_gains.absolute_gain_1m, plaid_gains.absolute_gain_1m)          as absolute_gain_1m,
                    coalesce(drivewealth_portfolio_holding_gains.absolute_gain_3m, plaid_gains.absolute_gain_3m)          as absolute_gain_3m,
                    coalesce(drivewealth_portfolio_holding_gains.absolute_gain_1y, plaid_gains.absolute_gain_1y)          as absolute_gain_1y,
                    coalesce(drivewealth_portfolio_holding_gains.absolute_gain_5y, plaid_gains.absolute_gain_5y)          as absolute_gain_5y,
                    coalesce(drivewealth_portfolio_holding_gains.absolute_gain_total, plaid_gains.absolute_gain_total)    as absolute_gain_total
             from {{ ref('profile_holdings_normalized') }}
                      left join {{ ref('ticker_realtime_metrics') }} using (symbol)
                      left join plaid_gains using (holding_id_v2)
                      left join plaid_holdings using (holding_id_v2)
                      left join {{ ref('drivewealth_portfolio_holding_gains') }} using (holding_id_v2)
                      left join {{ ref('drivewealth_portfolio_holding_gains_realtime') }} using (holding_id_v2)
    )
select holding_group_id,
       holding_id_v2,
       holding_id,
       profile_id,
       updated_at,
       actual_value,
       (actual_value / (1e-9 + sum(actual_value) over (partition by profile_id)))::double precision as value_to_portfolio_value,
       relative_gain_1d::double precision,
       case
           when abs(actual_value - absolute_gain_1w) > 1e-9
               then absolute_gain_1w / (actual_value - absolute_gain_1w)
           end::double precision       as relative_gain_1w,
       case
           when abs(actual_value - absolute_gain_1m) > 1e-9
               then absolute_gain_1m / (actual_value - absolute_gain_1m)
           end::double precision       as relative_gain_1m,
       case
           when abs(actual_value - absolute_gain_3m) > 1e-9
               then absolute_gain_3m / (actual_value - absolute_gain_3m)
           end::double precision       as relative_gain_3m,
       case
           when abs(actual_value - absolute_gain_1y) > 1e-9
               then absolute_gain_1y / (actual_value - absolute_gain_1y)
           end::double precision       as relative_gain_1y,
       case
           when abs(actual_value - absolute_gain_5y) > 1e-9
               then absolute_gain_5y / (actual_value - absolute_gain_5y)
           end::double precision       as relative_gain_5y,
       case
           when abs(actual_value - absolute_gain_total) > 1e-9
               then absolute_gain_total / (actual_value - absolute_gain_total)
           end::double precision as relative_gain_total,
       absolute_gain_1d,
       absolute_gain_1w,
       absolute_gain_1m,
       absolute_gain_3m,
       absolute_gain_1y,
       absolute_gain_5y,
       absolute_gain_total,
       coalesce(long_term_tax_holdings.ltt_quantity_total, 0)                                       as ltt_quantity_total
from {{ ref('profile_holdings_normalized') }}
         left join combined_gains using (holding_id_v2)
         left join long_term_tax_holdings using (holding_id_v2)
