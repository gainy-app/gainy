with filtered_holdings as
         (
             select distinct holding_id_v2
             from profile_holdings_normalized_all
                      {join_clause}
             where profile_holdings_normalized_all.profile_id = %(profile_id)s
                   {holding_where_clause}
         ),
     ticker_chart as
         (
             select profile_id,
                    portfolio_holding_chart.quantity,
                    period,
                    portfolio_holding_chart.date,
                    portfolio_holding_chart.datetime,
                    transaction_count,
                    cash_adjustment,
                    open,
                    high,
                    low,
                    close,
                    adjusted_close,
                    relative_gain
             from portfolio_holding_chart
                      left join filtered_holdings using (holding_id_v2)
             where profile_id = %(profile_id)s
               and filtered_holdings.holding_id_v2 is not null
                   {chart_where_clause}
         ),
     static_cash as
         (
             select profile_id,
                    holding_since as dt_from,
                    quantity      as value
             from profile_holdings_normalized_all
             where profile_id = %(profile_id)s
               and type = 'cash'
               and ticker_symbol = 'CUR:USD'
               and not is_app_trading

             union all

             select profile_id, created_at as dt_from, amount as value
             from app.trading_money_flow
             where profile_id = %(profile_id)s
               and amount > 0
               and status not in ('FAILED', 'SUCCESS')
     ),
     portfolio_chart_skeleton_with_cash as
         (
             select portfolio_chart_skeleton.profile_id,
                    period,
                    datetime,
                    case
                        when %(include_cash)s
                            then sum(value)
                        end as cash_value
             from portfolio_chart_skeleton
                      left join static_cash
                                on static_cash.profile_id = portfolio_chart_skeleton.profile_id
                                    and static_cash.dt_from >= portfolio_chart_skeleton.datetime
             group by portfolio_chart_skeleton.profile_id, period, datetime
         ),
     raw_chart as
         (
             select profile_id,
                    period,
                    max(date)                           as date,
                    datetime,
                    sum(transaction_count)              as transaction_count,
                    sum(open)                           as open,
                    sum(high)                           as high,
                    sum(low)                            as low,
                    sum(close)                          as close,
                    sum(adjusted_close)                 as adjusted_close,
                    sum(abs(adjusted_close))            as adjusted_close_abs,
                    sum(relative_gain * adjusted_close) as relative_gain,
                    sum(cash_adjustment)                as cash_adjustment
             from ticker_chart
             group by profile_id, period, datetime
             having (period != '1w' or max(date) >= now()::date - interval '1 week')
                and (period != '1m' or max(date) >= now()::date - interval '1 month')
                and (period != '3m' or max(date) >= now()::date - interval '3 month')
                and (period != '1y' or max(date) >= now()::date - interval '1 year')
                and (period != '5y' or max(date) >= now()::date - interval '5 year')
         )
select *
from (
         select period,
                rank() over (partition by profile_id, period order by raw_chart.date desc) = 1     as is_latest_day,
                datetime,
                transaction_count,
                (open + greatest(0, cash_adjustment + coalesce(cash_value, 0)))::double precision  as open,
                (high + greatest(0, cash_adjustment + coalesce(cash_value, 0)))::double precision  as high,
                (low + greatest(0, cash_adjustment + coalesce(cash_value, 0)))::double precision   as low,
                (close + greatest(0, cash_adjustment + coalesce(cash_value, 0)))::double precision as close,
                (adjusted_close +
                 greatest(0, cash_adjustment + coalesce(cash_value, 0)))::double precision         as adjusted_close,
                exp(sum(ln(coalesce(case
                                        when adjusted_close_abs > 0
                                            then relative_gain / adjusted_close_abs
                                        end, 0) + 1)) over wnd) - 1                                as relative_gain
         from raw_chart
                  join portfolio_chart_skeleton_with_cash using (profile_id, period, datetime)
         window wnd as (partition by period order by datetime)
     ) t
where (period != '1d' or is_latest_day)
