with filtered_transactions as
         (
             select portfolio_expanded_transactions.transaction_uniq_id,
                    portfolio_expanded_transactions.symbol,
                    sum(portfolio_expanded_transactions.quantity_norm_for_valuation) as quantity_norm_for_valuation
             from portfolio_expanded_transactions
                      {join_clause}
             where portfolio_expanded_transactions.profile_id = %(profile_id)s
                   {transaction_where_clause}
             group by portfolio_expanded_transactions.transaction_uniq_id, portfolio_expanded_transactions.symbol
         ),
     raw_chart_data as
         (
             select profile_id,
                    filtered_transactions.symbol,
                    filtered_transactions.quantity_norm_for_valuation,
                    period,
                    week_trading_session_index,
                    latest_trading_time,
                    portfolio_transaction_chart.date,
                    portfolio_transaction_chart.datetime,
                    transaction_uniq_id,
                    open,
                    high,
                    low,
                    close,
                    adjusted_close
             from portfolio_transaction_chart
                      join filtered_transactions using (transaction_uniq_id)
             where profile_id = %(profile_id)s
                   {chart_where_clause}
         ),
     ticker_chart as
         (
             select symbol,
                    period,
                    min(week_trading_session_index)  as week_trading_session_index,
                    min(latest_trading_time)         as latest_trading_time,
                    datetime,
                    min(date)                        as date,
                    sum(quantity_norm_for_valuation) as quantity,
                    count(transaction_uniq_id)       as transaction_count,
                    sum(open)                        as open,
                    sum(high)                        as high,
                    sum(low)                         as low,
                    sum(close)                       as close,
                    sum(adjusted_close)              as adjusted_close
             from raw_chart_data
             group by symbol, period, datetime
         ),
     schedule as materialized
         (
             select distinct period, datetime
             from (
                      select t.*,
                             max(cnt)
                             over (partition by period order by datetime rows between unbounded preceding and current row ) as cum_max_cnt
                      from (
                               select period,
                                      datetime,
                                      count(distinct symbol) as cnt
                               from ticker_chart
                               group by period, datetime
                           ) t
                  ) t
             where cnt = cum_max_cnt
     ),
     ticker_chart_with_cash_adjustment as
         (
             select ticker_chart.*,
                    case
                        when ticker_chart.quantity > 0
                            then ticker_chart.adjusted_close / ticker_chart.quantity *
                                 (last_value(quantity)
                                  over (partition by period, symbol order by datetime rows between current row and unbounded following) -
                                  ticker_chart.quantity)
                        else 0
                        end as cash_adjustment
             from ticker_chart
                      join schedule using (period, datetime)
     ),
     static_values as
         (
             select sum(value) as cash_value
             from (
                      select distinct on (
                          profile_holdings_normalized.holding_id_v2
                          ) profile_id,
                            case
                                when type = 'cash' and ticker_symbol = 'CUR:USD'
                                    then quantity::numeric
                                else 0
                                end as value
                      from profile_holdings_normalized
                      where profile_id = %(profile_id)s
                        and type = 'cash'
                        and ticker_symbol = 'CUR:USD'
                  ) t
     ),
     raw_data as
         (
             select shown_period                                           as period,
                    date,
                    sum(ticker_chart_with_cash_adjustment.adjusted_close)  as adjusted_close,
                    sum(ticker_chart_with_cash_adjustment.cash_adjustment) as cash_adjustment
             from (
                      select period        as shown_period,
                             case
                                 when period in ('1d', '1m', '1w')
                                     then '1y'
                                 else period
                                 end       as period,
                             max(date)     as date,
                             max(datetime) as datetime
                      from ticker_chart_with_cash_adjustment
                      where (period != '1d' or week_trading_session_index > 0)
                        and (period != '1w' or date < now() - interval '1 week')
                        and (period != '1m' or datetime < latest_trading_time::date - interval '1 month')
                        and (period != '3m' or datetime < latest_trading_time::date - interval '3 month')
                        and (period != '1y' or datetime < latest_trading_time::date - interval '1 year')
                        and (period != '5y' or datetime < latest_trading_time::date - interval '5 year')
                      group by period
                  ) t
                      left join ticker_chart_with_cash_adjustment using (period, date)
             group by shown_period, date
     )
select raw_data_1d.adjusted_close + greatest(0, coalesce(raw_data_1d.cash_adjustment, 0) + coalesce(cash_value, 0)) as prev_close_1d,
       raw_data_1w.adjusted_close + greatest(0, coalesce(raw_data_1w.cash_adjustment, 0) + coalesce(cash_value, 0)) as prev_close_1w,
       raw_data_1m.adjusted_close + greatest(0, coalesce(raw_data_1m.cash_adjustment, 0) + coalesce(cash_value, 0)) as prev_close_1m,
       raw_data_3m.adjusted_close + greatest(0, coalesce(raw_data_3m.cash_adjustment, 0) + coalesce(cash_value, 0)) as prev_close_3m,
       raw_data_1y.adjusted_close + greatest(0, coalesce(raw_data_1y.cash_adjustment, 0) + coalesce(cash_value, 0)) as prev_close_1y,
       raw_data_5y.adjusted_close + greatest(0, coalesce(raw_data_5y.cash_adjustment, 0) + coalesce(cash_value, 0)) as prev_close_5y
from static_values
         left join raw_data raw_data_1d on raw_data_1d.period = '1d'
         left join raw_data raw_data_1w on raw_data_1w.period = '1w'
         left join raw_data raw_data_1m on raw_data_1m.period = '1m'
         left join raw_data raw_data_3m on raw_data_3m.period = '3m'
         left join raw_data raw_data_1y on raw_data_1y.period = '1y'
         left join raw_data raw_data_5y on raw_data_5y.period = '5y'
