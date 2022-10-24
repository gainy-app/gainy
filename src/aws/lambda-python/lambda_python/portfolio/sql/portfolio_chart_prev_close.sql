with filtered_transactions as
         (
             select portfolio_expanded_transactions.uniq_id as transaction_uniq_id,
                    portfolio_expanded_transactions.profile_id,
                    portfolio_securities_normalized.original_ticker_symbol as symbol,
                    sum(portfolio_expanded_transactions.quantity_norm_for_valuation) as quantity_norm_for_valuation,
                    min(portfolio_expanded_transactions.date)                    as datetime
             from portfolio_expanded_transactions
                      join portfolio_securities_normalized
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                      join app.profile_portfolio_accounts on profile_portfolio_accounts.id = portfolio_expanded_transactions.account_id
                      join app.profile_plaid_access_tokens on profile_plaid_access_tokens.id = profile_portfolio_accounts.plaid_access_token_id
                      {join_clause}
             where {transaction_where_clause}
             group by portfolio_expanded_transactions.uniq_id, portfolio_expanded_transactions.profile_id,
                      portfolio_securities_normalized.original_ticker_symbol
         ),
     raw_chart_data as
         (
             select filtered_transactions.profile_id,
                    filtered_transactions.symbol,
                    filtered_transactions.quantity_norm_for_valuation,
                    period,
                    week_trading_sessions_static.index as week_trading_session_index,
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
                      left join week_trading_sessions_static
                                on week_trading_sessions_static.symbol = filtered_transactions.symbol
                                    and portfolio_transaction_chart.datetime between week_trading_sessions_static.open_at and week_trading_sessions_static.close_at - interval '1 millisecond'
             where ((period in ('1d', '1w') and portfolio_transaction_chart.datetime between week_trading_sessions_static.open_at and week_trading_sessions_static.close_at)
                 or (period = '1m' and portfolio_transaction_chart.datetime >= ticker_realtime_metrics.time::date - interval '1 month')
                 or (period = '3m' and portfolio_transaction_chart.datetime >= ticker_realtime_metrics.time::date - interval '3 months')
                 or (period = '1y' and portfolio_transaction_chart.datetime >= ticker_realtime_metrics.time::date - interval '1 year')
                 or (period = '5y' and portfolio_transaction_chart.datetime >= ticker_realtime_metrics.time::date - interval '5 years')
                 or (period = 'all'))
               and {chart_where_clause}
         ),
     ticker_chart as
         (
             select profile_id,
                    symbol,
                    period,
                    min(week_trading_session_index)   as week_trading_session_index,
                    datetime,
                    sum(quantity_norm_for_valuation)  as quantity,
                    count(transaction_uniq_id)        as transaction_count,
                    sum(open)                         as open,
                    sum(high)                         as high,
                    sum(low)                          as low,
                    sum(close)                        as close,
                    sum(adjusted_close)               as adjusted_close
             from raw_chart_data
             group by profile_id, symbol, period, datetime
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
             select profile_id, period, datetime
             from ticker_chart
                      left join datetimes_to_exclude_from_schedule using (period, datetime)
                      left join symbols_to_exclude_from_schedule using (period, symbol)
             where (period = '1d' and datetimes_to_exclude_from_schedule.datetime is null)
                or (period != '1d' and symbols_to_exclude_from_schedule.symbol is null)
             group by profile_id, period, datetime
     ),
     ticker_chart_with_cash_adjustment as
         (
             select ticker_chart.*,
                    case
                        when ticker_chart.quantity > 0
                            then ticker_chart.adjusted_close / ticker_chart.quantity *
                                 (last_value(quantity) over (partition by profile_id, period, symbol order by datetime rows between current row and unbounded following) - ticker_chart.quantity)
                        else 0
                        end as cash_adjustment
             from ticker_chart
                      join schedule using (profile_id, period, datetime)
         ),
     static_values as
         (
             with raw_data as
                      (
                          select distinct on (
                              profile_holdings_normalized.holding_id
                              ) profile_holdings_normalized.profile_id,
                                case
                                    when portfolio_securities_normalized.type = 'cash'
                                        and portfolio_securities_normalized.ticker_symbol = 'CUR:USD'
                                        then profile_holdings_normalized.quantity::numeric
                                    else 0
                                    end as value
                          from profile_holdings_normalized
                                   join portfolio_securities_normalized
                                        on portfolio_securities_normalized.id = profile_holdings_normalized.security_id
                                   join app.profile_portfolio_accounts
                                        on profile_portfolio_accounts.id = profile_holdings_normalized.account_id
                                   join app.profile_plaid_access_tokens
                                        on profile_plaid_access_tokens.id =
                                           profile_portfolio_accounts.plaid_access_token_id
                          where portfolio_securities_normalized.type = 'cash'
                            and portfolio_securities_normalized.ticker_symbol = 'CUR:USD'
                      )
             select profile_id,
                    sum(value) as cash_value
             from raw_data
             group by profile_id
         ),
     raw_chart as materialized
         (
             select profile_id,
                    period,
                    datetime,
                    sum(cash_adjustment) as cash_adjustment
             from ticker_chart_with_cash_adjustment
             group by profile_id, period, datetime
             having (period != '1d' or min(week_trading_session_index) = 0)
     ),
     cash_adjustments as materialized
         (
             select profile_id, period, datetime, cash_adjustment
             from (
                      select profile_id, period, min(datetime) as datetime
                      from raw_chart
                      group by profile_id, period
                  ) t
                      join raw_chart using (profile_id, period, datetime)
     ),
     raw_data_1d as
         (
             select filtered_transactions.profile_id,
                    sum(quantity_norm_for_valuation * historical_prices_marked.price_0d) as prev_close_1d,
                    sum(cash_adjustment)                                                 as cash_adjustment
             from filtered_transactions
                      join historical_prices_marked using (symbol)
                      left join cash_adjustments
                                on cash_adjustments.profile_id = filtered_transactions.profile_id
                                    and period = '1d'
             group by filtered_transactions.profile_id
     ),
     raw_data_1w as
         (
             select filtered_transactions.profile_id,
                    sum(quantity_norm_for_valuation * historical_prices_marked.price_1w) as prev_close_1w,
                    sum(cash_adjustment)                                                 as cash_adjustment
             from filtered_transactions
                      join historical_prices_marked using (symbol)
                      left join cash_adjustments
                                on cash_adjustments.profile_id = filtered_transactions.profile_id
                                    and period = '1w'
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
                      left join cash_adjustments
                                on cash_adjustments.profile_id = filtered_transactions.profile_id
                                    and period = '1m'
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
                      left join cash_adjustments
                                on cash_adjustments.profile_id = filtered_transactions.profile_id
                                    and period = '3m'
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
                      left join cash_adjustments
                                on cash_adjustments.profile_id = filtered_transactions.profile_id
                                    and period = '1y'
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
                      left join cash_adjustments
                                on cash_adjustments.profile_id = filtered_transactions.profile_id
                                    and period = '5y'
             where filtered_transactions.datetime <= historical_prices_marked.date_5y
             group by filtered_transactions.profile_id
     )
select profile_id,
       prev_close_1d + greatest(0, raw_data_1d.cash_adjustment + coalesce(cash_value, 0)) as prev_close_1d,
       prev_close_1w + greatest(0, raw_data_1w.cash_adjustment + coalesce(cash_value, 0)) as prev_close_1w,
       prev_close_1m + greatest(0, raw_data_1m.cash_adjustment + coalesce(cash_value, 0)) as prev_close_1m,
       prev_close_3m + greatest(0, raw_data_3m.cash_adjustment + coalesce(cash_value, 0)) as prev_close_3m,
       prev_close_1y + greatest(0, raw_data_1y.cash_adjustment + coalesce(cash_value, 0)) as prev_close_1y,
       prev_close_5y + greatest(0, raw_data_5y.cash_adjustment + coalesce(cash_value, 0)) as prev_close_5y
from raw_data_1d
         left join static_values using (profile_id)
         left join raw_data_1w using (profile_id)
         left join raw_data_1m using (profile_id)
         left join raw_data_3m using (profile_id)
         left join raw_data_1y using (profile_id)
         left join raw_data_5y using (profile_id)