with filtered_transactions as
         (
             select portfolio_expanded_transactions.transaction_uniq_id,
                    portfolio_expanded_transactions.profile_id,
                    portfolio_expanded_transactions.holding_id_v2,
                    portfolio_expanded_transactions.symbol,
                    sum(portfolio_expanded_transactions.quantity_norm_for_valuation) as quantity_norm_for_valuation
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
     schedule as materialized
         (
             select profile_id, period, datetime
             from ticker_chart
                      left join datetimes_to_exclude_from_schedule using (period, datetime)
             where datetimes_to_exclude_from_schedule.datetime is null
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
                              holding_id_v2
                              ) profile_id,
                                case
                                    when type = 'cash' and ticker_symbol = 'CUR:USD'
                                        then quantity::numeric
                                    else 0
                                    end as value
                          from profile_holdings_normalized
                          where type = 'cash'
                            and ticker_symbol = 'CUR:USD'
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
                     sum(transaction_count) as transaction_count,
                     sum(open)              as open,
                     sum(high)              as high,
                     sum(low)               as low,
                     sum(close)             as close,
                     sum(adjusted_close)    as adjusted_close,
                     sum(cash_adjustment)   as cash_adjustment
              from ticker_chart_with_cash_adjustment
              group by profile_id, period, datetime
              having (period != '1d' or min(week_trading_session_index) = 0)
         )
select period,
       datetime,
       transaction_count,
       (open + greatest(0, cash_adjustment + coalesce(cash_value, 0)))::double precision           as open,
       (high + greatest(0, cash_adjustment + coalesce(cash_value, 0)))::double precision           as high,
       (low + greatest(0, cash_adjustment + coalesce(cash_value, 0)))::double precision            as low,
       (close + greatest(0, cash_adjustment + coalesce(cash_value, 0)))::double precision          as close,
       (adjusted_close + greatest(0, cash_adjustment + coalesce(cash_value, 0)))::double precision as adjusted_close
from raw_chart
         left join static_values using (profile_id)
