with week_trading_sessions as
         (
             select min(open_at)                           as open_at,
                    min(close_at)                          as close_at,
                    min(date)                              as date,
                    row_number() over (order by date desc) as idx
             from exchange_schedule
             where open_at between now() - interval '1 week' and now()
             group by date
         ),
     latest_open_trading_session as
         (
             select *
             from week_trading_sessions
             where idx = 1
         ),
     raw_chart_data as
         (
             select distinct on (uniq_id, period, portfolio_transaction_chart.datetime)
                 portfolio_expanded_transactions.profile_id,
                 original_ticker_symbol,
                 quantity_norm,
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
                      left join week_trading_sessions
                                on portfolio_transaction_chart.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
                      left join latest_open_trading_session on true
                      join portfolio_securities_normalized
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                      join app.profile_portfolio_accounts on profile_portfolio_accounts.id = portfolio_expanded_transactions.account_id
                      join app.profile_plaid_access_tokens on profile_plaid_access_tokens.id = profile_portfolio_accounts.plaid_access_token_id
                      {join_clause}
             where ((period = '1d' and week_trading_sessions.idx = 1)
                 or (period = '1w' and week_trading_sessions.idx is not null)
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
                    original_ticker_symbol,
                    period,
                    datetime,
                    sum(quantity_norm)  as quantity,
                    count(uniq_id)      as transaction_count,
                    sum(open)           as open,
                    sum(high)           as high,
                    sum(low)            as low,
                    sum(close)          as close,
                    sum(adjusted_close) as adjusted_close
             from raw_chart_data
             group by profile_id, original_ticker_symbol, period, datetime
         ),
     ticker_chart_latest_datapoint as
         (
             select ticker_chart.*
             from (
                      select original_ticker_symbol,
                             period,
                             max(datetime) as datetime
                      from raw_chart_data
                      group by original_ticker_symbol, period
                  ) latest_datapoint
                      join ticker_chart using (original_ticker_symbol, period, datetime)
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
                      join ticker_chart_latest_datapoint using (profile_id, original_ticker_symbol, period)
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
         )
select period,
       datetime,
       transaction_count,
       open + greatest(0, cash_adjustment + coalesce(cash_value, 0))           as open,
       high + greatest(0, cash_adjustment + coalesce(cash_value, 0))           as high,
       low + greatest(0, cash_adjustment + coalesce(cash_value, 0))            as low,
       close + greatest(0, cash_adjustment + coalesce(cash_value, 0))          as close,
       adjusted_close + greatest(0, cash_adjustment + coalesce(cash_value, 0)) as adjusted_close
from (
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
     ) t
         left join static_values using (profile_id)
