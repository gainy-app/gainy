with latest_open_trading_session as
         (
             select symbol, max(date) as date
             from week_trading_sessions
             where index = 0
             group by symbol
         ),
     raw_chart_data as
         (
             select distinct on (uniq_id, period, portfolio_transaction_chart.datetime)
                 portfolio_expanded_transactions.profile_id,
                 profile_holdings_normalized.type,
                 portfolio_expanded_transactions.symbol,
                 portfolio_expanded_transactions.quantity_norm_for_valuation,
                 period,
                 portfolio_transaction_chart.datetime,
                 uniq_id,
                 open::numeric,
                 high::numeric,
                 low::numeric,
                 close::numeric,
                 adjusted_close::numeric
             from portfolio_transaction_chart
                      join portfolio_expanded_transactions
                           on portfolio_expanded_transactions.uniq_id = portfolio_transaction_chart.transactions_uniq_id
                      left join profile_holdings_normalized using (holding_id_v2)
                      left join week_trading_sessions
                                on week_trading_sessions.symbol = portfolio_expanded_transactions.symbol
                                    and portfolio_transaction_chart.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
                      left join latest_open_trading_session
                                on latest_open_trading_session.symbol = portfolio_expanded_transactions.symbol
                      left join app.profile_plaid_access_tokens on profile_plaid_access_tokens.id = profile_holdings_normalized.plaid_access_token_id
                      {join_clause}
             where ((period = '1d' and week_trading_sessions.index = 0)
                 or (period = '1w' and week_trading_sessions.index is not null)
                 or (period = '1m' and portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '1 month')
                 or (period = '3m' and portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '3 months')
                 or (period = '1y' and portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '1 year')
                 or (period = '5y' and portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '5 years')
                 or (period = 'all'))
               and {where_clause}
         ),
     ticker_chart as
         (
             select profile_id,
                    symbol,
                    period,
                    datetime,
                    sum(quantity_norm_for_valuation)  as quantity,
                    count(uniq_id)                    as transaction_count,
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
     symbols_to_exclude_from_schedule as
         (
             select distinct ticker_chart.symbol
             from (
                      select distinct period, datetime
                      from (
                               select period, datetime
                               from ticker_chart
                               group by period, datetime
                           ) t
                               left join chart_date_stats using (period)
                               left join ticker_chart using (symbol, period, datetime)
                      where ticker_chart is null
                        and (datetime between min_datetime and max_datetime or period = '1d')
                  ) t1
                      join ticker_chart using (period, datetime)
                      left join week_trading_sessions_static
                                on week_trading_sessions_static.symbol = ticker_chart.symbol
                                    and datetime >= week_trading_sessions_static.open_at
                                    and datetime < week_trading_sessions_static.close_at
             where week_trading_sessions_static is not null or period != '1w'
     ),
     schedule as  materialized
         (
             select profile_id, period, datetime

             from ticker_chart
                      left join symbols_to_exclude_from_schedule using (symbol)
             where symbols_to_exclude_from_schedule is null
             group by profile_id, period, datetime
     ),
     ticker_chart_latest_datapoint as  materialized
         (
             select ticker_chart.*
             from (
                      select symbol,
                             period,
                             max(datetime) as datetime
                      from raw_chart_data
                      group by symbol, period
                  ) latest_datapoint
                      join ticker_chart using (symbol, period, datetime)
         ),
     ticker_chart_with_cash_adjustment as
         (

             select ticker_chart.*,
                    case
                        when ticker_chart.quantity > 0
                            then ticker_chart.adjusted_close / ticker_chart.quantity *
                                 (ticker_chart_latest_datapoint.quantity - ticker_chart.quantity)
                        else 0
                        end as cash_adjustment
             from ticker_chart
                      join schedule using (profile_id, period, datetime)
                      join ticker_chart_latest_datapoint using (profile_id, symbol, period)
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
