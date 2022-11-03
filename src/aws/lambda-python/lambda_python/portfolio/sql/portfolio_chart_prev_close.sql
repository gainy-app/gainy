with filtered_transactions as
         (
             select portfolio_expanded_transactions.transaction_uniq_id,
                    portfolio_expanded_transactions.profile_id,
                    portfolio_expanded_transactions.holding_id_v2,
                    portfolio_expanded_transactions.symbol,
                    sum(portfolio_expanded_transactions.quantity_norm_for_valuation) as quantity_norm_for_valuation,
                    min(portfolio_expanded_transactions.datetime)                    as datetime
             from portfolio_expanded_transactions
                      left join profile_holdings_normalized using (holding_id_v2)
                      left join app.profile_plaid_access_tokens
                                on profile_plaid_access_tokens.id = profile_holdings_normalized.plaid_access_token_id
                      {join_clause}
             where {transaction_where_clause}
             group by portfolio_expanded_transactions.transaction_uniq_id, portfolio_expanded_transactions.profile_id,
                      portfolio_expanded_transactions.holding_id_v2, portfolio_expanded_transactions.symbol
         ),
     raw_chart_data as
         (
             select filtered_transactions.profile_id,
                    filtered_transactions.symbol,
                    filtered_transactions.quantity_norm_for_valuation,
                    period,
                    week_trading_session_index,
                    latest_trading_time,
                    portfolio_transaction_chart.datetime,
                    transaction_uniq_id,
                    open::numeric,
                    high::numeric,
                    low::numeric,
                    close::numeric,
                    adjusted_close::numeric
             from portfolio_transaction_chart
                      join filtered_transactions using (transaction_uniq_id, profile_id)
                      left join ticker_realtime_metrics using (symbol)
             where {chart_where_clause}
     ),
     ticker_chart as
         (
             select symbol,
                    period,
                    min(week_trading_session_index)  as week_trading_session_index,
                    min(latest_trading_time)         as latest_trading_time,
                    datetime,
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
     chart_date_stats as
         (
             select symbol,
                    period,
                    min(datetime) as min_datetime,
                    max(datetime) as max_datetime
             from raw_chart_data
             group by symbol, period
     ),
     datetimes_to_exclude_from_schedule as materialized
         (
             select period, datetime
             from (
                      select distinct symbol, period, datetime
                      from (
                               select period, datetime
                               from ticker_chart
                               group by period, datetime
                           ) t
                               join chart_date_stats using (period)
                               left join ticker_chart using (symbol, period, datetime)
                      where ticker_chart.symbol is null
                  ) t
                      join chart_date_stats using (symbol, period)
             where datetime between min_datetime and max_datetime
                or period = '1d'
             group by period, datetime
     ),
     symbols_to_exclude_from_schedule as
         (
             select distinct period, ticker_chart.symbol
             from datetimes_to_exclude_from_schedule
                      join ticker_chart using (period, datetime)
                      left join week_trading_sessions_static
                                on week_trading_sessions_static.symbol = ticker_chart.symbol
                                    and datetime >= week_trading_sessions_static.open_at
                                    and datetime < week_trading_sessions_static.close_at
             where week_trading_sessions_static is not null or period != '1w'
     ),
     schedule as materialized
         (
             select period, datetime
             from ticker_chart
                      left join datetimes_to_exclude_from_schedule using (period, datetime)
                      left join symbols_to_exclude_from_schedule using (period, symbol)
             where (period = '1d' and datetimes_to_exclude_from_schedule.datetime is null)
                or (period != '1d' and symbols_to_exclude_from_schedule.symbol is null)
             group by period, datetime
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
             with raw_data as
                      (
                          select distinct on (
                              holding_id_v2
                              ) profile_id,
                                case
                                    when type = 'cash' and ticker_symbol = 'CUR:USD'
                                        then quantity::numeric
                                    else 0
                                    end as value
                          from profile_holdings_normalized
                          where profile_id in (select profile_id from filtered_transactions group by profile_id)
                            and type = 'cash'
                            and ticker_symbol = 'CUR:USD'
                      )
             select sum(value) as cash_value
             from raw_data
     ),
     raw_chart as materialized
         (
             select period,
                    datetime,
                    sum(cash_adjustment) as cash_adjustment
             from ticker_chart_with_cash_adjustment
             group by period, datetime
             having (period != '1d' or min(week_trading_session_index) = 0)
                and (period != '1w' or min(week_trading_session_index) < 7)
                and (period != '1m' or datetime >= min(latest_trading_time)::date - interval '1 month')
                and (period != '3m' or datetime >= min(latest_trading_time)::date - interval '3 month')
                and (period != '1y' or datetime >= min(latest_trading_time)::date - interval '1 year')
                and (period != '5y' or datetime >= min(latest_trading_time)::date - interval '5 year')
     ),
     cash_adjustments as materialized
         (
             select period, datetime, cash_adjustment
             from (
                      select period, min(datetime) as datetime
                      from raw_chart
                      group by period
                  ) t
                      join raw_chart using (period, datetime)
     ),
     raw_data_1d as
         (
             select filtered_transactions.profile_id,
                    sum(quantity_norm_for_valuation * historical_prices_marked.price_0d) as prev_close_1d,
                    sum(cash_adjustment)                                                 as cash_adjustment
             from filtered_transactions
                      join historical_prices_marked using (symbol)
                      left join cash_adjustments on period = '1d'
             group by filtered_transactions.profile_id
     ),
     raw_data_1w as
         (
             select filtered_transactions.profile_id,
                    sum(quantity_norm_for_valuation * historical_prices_marked.price_1w) as prev_close_1w,
                    sum(cash_adjustment)                                                 as cash_adjustment
             from filtered_transactions
                      join historical_prices_marked using (symbol)
                      left join cash_adjustments on period = '1w'
             where filtered_transactions.datetime <= historical_prices_marked.date_1w
             group by filtered_transactions.profile_id
     ),
     raw_data_1m as
         (
             select filtered_transactions.profile_id,
                    sum(quantity_norm_for_valuation * historical_prices_marked.price_1m) as prev_close_1m,
                    sum(cash_adjustment)                                                 as cash_adjustment
             from filtered_transactions
                      join historical_prices_marked using (symbol)
                      left join cash_adjustments on period = '1m'
             where filtered_transactions.datetime <= historical_prices_marked.date_1m
             group by filtered_transactions.profile_id
     ),
     raw_data_3m as
         (
             select filtered_transactions.profile_id,
                    sum(quantity_norm_for_valuation * historical_prices_marked.price_3m) as prev_close_3m,
                    sum(cash_adjustment)                                                 as cash_adjustment
             from filtered_transactions
                      join historical_prices_marked using (symbol)
                      left join cash_adjustments on period = '3m'
             where filtered_transactions.datetime <= historical_prices_marked.date_3m
             group by filtered_transactions.profile_id
     ),
     raw_data_1y as
         (
             select filtered_transactions.profile_id,
                    sum(quantity_norm_for_valuation * historical_prices_marked.price_1y) as prev_close_1y,
                    sum(cash_adjustment)                                                 as cash_adjustment
             from filtered_transactions
                      join historical_prices_marked using (symbol)
                      left join cash_adjustments on period = '1y'
             where filtered_transactions.datetime <= historical_prices_marked.date_1y
             group by filtered_transactions.profile_id
     ),
     raw_data_5y as
         (
             select filtered_transactions.profile_id,
                    sum(quantity_norm_for_valuation * historical_prices_marked.price_5y) as prev_close_5y,
                    sum(cash_adjustment)                                                 as cash_adjustment
             from filtered_transactions
                      join historical_prices_marked using (symbol)
                      left join cash_adjustments on period = '5y'
             where filtered_transactions.datetime <= historical_prices_marked.date_5y
             group by filtered_transactions.profile_id
     )
select prev_close_1d + greatest(0, raw_data_1d.cash_adjustment + coalesce(cash_value, 0)) as prev_close_1d,
       prev_close_1w + greatest(0, raw_data_1w.cash_adjustment + coalesce(cash_value, 0)) as prev_close_1w,
       prev_close_1m + greatest(0, raw_data_1m.cash_adjustment + coalesce(cash_value, 0)) as prev_close_1m,
       prev_close_3m + greatest(0, raw_data_3m.cash_adjustment + coalesce(cash_value, 0)) as prev_close_3m,
       prev_close_1y + greatest(0, raw_data_1y.cash_adjustment + coalesce(cash_value, 0)) as prev_close_1y,
       prev_close_5y + greatest(0, raw_data_5y.cash_adjustment + coalesce(cash_value, 0)) as prev_close_5y
from raw_data_1d
         left join static_values on true
         left join raw_data_1w on true
         left join raw_data_1m on true
         left join raw_data_3m on true
         left join raw_data_1y on true
         left join raw_data_5y on true