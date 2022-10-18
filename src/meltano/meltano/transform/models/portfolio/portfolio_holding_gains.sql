{{
  config(
    materialized = "incremental",
    unique_key = "holding_id",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id'),
    ]
  )
}}


with relative_data as
         (
             select holding_group_id,
                    holding_id,
                    profile_holdings_normalized.profile_id,
                    now()::timestamp                              as updated_at,
                    ticker_realtime_metrics.actual_price          as actual_price,

                    case
                        when portfolio_securities_normalized.type = 'cash' and
                             portfolio_securities_normalized.original_ticker_symbol = 'CUR:USD'
                            then quantity
                        else ticker_realtime_metrics.actual_price * quantity_norm_for_valuation
                        end                                       as actual_value,
                    ticker_realtime_metrics.relative_daily_change as relative_gain_1d,
                    ticker_realtime_metrics.absolute_daily_change as absolute_gain_1d,
                    ticker_metrics.price_change_1w                as relative_gain_1w,
                    ticker_metrics.price_change_1m                as relative_gain_1m,
                    ticker_metrics.price_change_3m                as relative_gain_3m,
                    ticker_metrics.price_change_1y                as relative_gain_1y,
                    ticker_metrics.price_change_5y                as relative_gain_5y,
                    ticker_metrics.price_change_all               as relative_gain_total,
                    profile_holdings_normalized.quantity::numeric
             from {{ ref('profile_holdings_normalized') }}
                      join {{ ref('portfolio_securities_normalized') }}
                           on portfolio_securities_normalized.id = profile_holdings_normalized.security_id
                      left join {{ ref('ticker_metrics') }}
                                on ticker_metrics.symbol = portfolio_securities_normalized.original_ticker_symbol
                      left join {{ ref('ticker_realtime_metrics') }}
                                on ticker_realtime_metrics.symbol = portfolio_securities_normalized.original_ticker_symbol
         ),
     long_term_tax_holdings as
         (
             select distinct on (
                 holding_id
                 ) holding_id,
                   ltt_quantity_total::double precision
             from (
                      select profile_holdings_normalized.holding_id,
                             quantity_sign,
                             datetime,
                             min(cumsum) over wnd as ltt_quantity_total
                      from (
                               select portfolio_expanded_transactions.profile_id,
                                      security_id,
                                      portfolio_expanded_transactions.account_id,
                                      datetime,
                                      sign(quantity_norm)                                                                                                    as quantity_sign,
                                      sum(quantity_norm)
                                      over (partition by security_id, portfolio_expanded_transactions.profile_id order by sign(quantity_norm), datetime)     as cumsum
                               from {{ ref('portfolio_expanded_transactions') }}
                               where portfolio_expanded_transactions.profile_id is not null
                           ) t
                               join {{ ref('profile_holdings_normalized') }}
                                    on profile_holdings_normalized.profile_id = t.profile_id
                                        and profile_holdings_normalized.security_id = t.security_id
                                        and profile_holdings_normalized.account_id = t.account_id
                      window wnd as (partition by t.profile_id, t.security_id
                                     order by t.quantity_sign, datetime
                                     rows between current row and unbounded following)
                  ) t
             where datetime < now() - interval '1 year'
             order by holding_id, quantity_sign desc, datetime desc
     ),
     all_rows as
         (
             select holding_group_id,
                    holding_id,
                    profile_id,
                    updated_at,
                    actual_value,
                    relative_gain_1d::double precision,
                    relative_gain_1w::double precision,
                    relative_gain_1m::double precision,
                    relative_gain_3m::double precision,
                    relative_gain_1y::double precision,
                    relative_gain_5y::double precision,
                    relative_gain_total::double precision,
                    absolute_gain_1d::double precision,
                    (actual_price * (1 - 1 / (1 + relative_gain_1w)))::double precision    as absolute_gain_1w,
                    (actual_price * (1 - 1 / (1 + relative_gain_1m)))::double precision    as absolute_gain_1m,
                    (actual_price * (1 - 1 / (1 + relative_gain_3m)))::double precision    as absolute_gain_3m,
                    (actual_price * (1 - 1 / (1 + relative_gain_1y)))::double precision    as absolute_gain_1y,
                    (actual_price * (1 - 1 / (1 + relative_gain_5y)))::double precision    as absolute_gain_5y,
                    (actual_price * (1 - 1 / (1 + relative_gain_total)))::double precision as absolute_gain_total,
                    coalesce(long_term_tax_holdings.ltt_quantity_total, 0)                 as ltt_quantity_total
             from relative_data
                      left join long_term_tax_holdings using (holding_id)

             union all

             (
                 with expanded_holding_groups as
                          (
                              select drivewealth_holdings.holding_group_id,
                                     holding_id,
                                     drivewealth_holdings.profile_id,
                                     drivewealth_holdings.collection_id,
                                     drivewealth_holdings.symbol,
                                     drivewealth_holdings.updated_at,
                                     drivewealth_holdings.actual_value::numeric
                              from {{ ref('drivewealth_holdings') }}
                                       join {{ ref('profile_holdings_normalized') }} using (holding_id)
                          ),
                      expanded_holding_groups_with_gains as
                          (
                              select holding_group_id,
                                     holding_id,
                                     expanded_holding_groups.profile_id,
                                     expanded_holding_groups.collection_id,
                                     expanded_holding_groups.updated_at,
                                     expanded_holding_groups.actual_value,
                                     ticker_realtime_metrics.actual_price,
                                     ticker_realtime_metrics.relative_daily_change as relative_gain_1d,
                                     ticker_metrics.price_change_1w       as relative_gain_1w,
                                     ticker_metrics.price_change_1m       as relative_gain_1m,
                                     ticker_metrics.price_change_3m       as relative_gain_3m,
                                     ticker_metrics.price_change_1y       as relative_gain_1y,
                                     ticker_metrics.price_change_5y       as relative_gain_5y,
                                     ticker_metrics.price_change_all      as relative_gain_total
                              from expanded_holding_groups
                                       left join {{ ref('ticker_metrics') }} using (symbol)
                                       left join {{ ref('ticker_realtime_metrics') }} using (symbol)
                      )
                 select holding_group_id,
                        holding_id,
                        profile_id,
                        updated_at,
                        actual_value::double precision,
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
                        (actual_price * (1 - 1 / (1 + relative_gain_total)))::double precision as absolute_gain_total,
                        null::double precision as ltt_quantity_total
                 from expanded_holding_groups_with_gains
             )
     )
select all_rows.*,
       (actual_value / (1e-9 + sum(actual_value) over (partition by profile_id)))::double precision as value_to_portfolio_value
from all_rows
