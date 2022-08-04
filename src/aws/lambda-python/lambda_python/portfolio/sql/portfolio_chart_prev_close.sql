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
             select distinct on (
                 uniq_id, period, portfolio_transaction_chart.datetime
                 ) portfolio_expanded_transactions.profile_id,
                   original_ticker_symbol,
                   quantity_norm_for_valuation,
                   period,
                   portfolio_transaction_chart.datetime,
                   adjusted_close::numeric
             from portfolio_transaction_chart
                      join portfolio_expanded_transactions
                           on portfolio_expanded_transactions.uniq_id = portfolio_transaction_chart.transactions_uniq_id
                      left join week_trading_sessions
                                on portfolio_transaction_chart.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
                      left join latest_open_trading_session on true
                      join portfolio_securities_normalized
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                      join app.profile_portfolio_accounts
                           on profile_portfolio_accounts.id = portfolio_expanded_transactions.account_id
                      join app.profile_plaid_access_tokens
                           on profile_plaid_access_tokens.id = profile_portfolio_accounts.plaid_access_token_id
                      join historical_prices_marked
                           on historical_prices_marked.symbol = original_ticker_symbol
                      {join_clause}
             where ((period = '1d' and week_trading_sessions.idx = 1)
                 or (period = '1w'
                         and week_trading_sessions.idx is not null
                         and portfolio_expanded_transactions.date <= historical_prices_marked.date_1w)
                 or (period = '1m'
                         and portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '1 month'
                         and portfolio_expanded_transactions.date <= historical_prices_marked.date_1m)
                 or (period = '3m'
                         and portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '3 months'
                         and portfolio_expanded_transactions.date <= historical_prices_marked.date_3m)
                 or (period = '1y'
                         and portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '1 year'
                         and portfolio_expanded_transactions.date <= historical_prices_marked.date_1y)
                 or (period = '5y'
                         and portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '5 years'
                         and portfolio_expanded_transactions.date <= historical_prices_marked.date_5y))
             and {where_clause}
         ),
     ticker_chart as
         (
             select profile_id,
                    original_ticker_symbol,
                    period,
                    datetime,
                    sum(quantity_norm_for_valuation) as quantity,
                    sum(adjusted_close)              as adjusted_close
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
         ),
     raw_data as
         (
             select distinct on (
                 profile_id, original_ticker_symbol, period
                 ) profile_id,
                   original_ticker_symbol,
                   period,
                   datetime,
                   quantity,
                   cash_adjustment + coalesce(cash_value, 0) as cash_value
             from (
                      select profile_id,
                             original_ticker_symbol,
                             period,
                             datetime,
                             sum(quantity)        as quantity,
                             sum(cash_adjustment) as cash_adjustment
                      from ticker_chart_with_cash_adjustment
                      group by profile_id, original_ticker_symbol, period, datetime
                  ) t
                      left join static_values using (profile_id)
             order by profile_id, original_ticker_symbol, period, datetime
         ),
     raw_data_1d as
         (
             select profile_id,
                    sum(quantity * historical_prices_marked.price_0d) as prev_close_1d
             from raw_data
                      join historical_prices_marked
                           on historical_prices_marked.symbol = raw_data.original_ticker_symbol
             where raw_data.period = '1d'
             group by profile_id
         ),
     raw_data_1w as
         (
             select profile_id,
                    sum(quantity * historical_prices_marked.price_1w) as prev_close_1w
             from raw_data
                      join historical_prices_marked
                           on historical_prices_marked.symbol = raw_data.original_ticker_symbol
             where raw_data.period = '1w'
             group by profile_id
         ),
     raw_data_1m as
         (
             select profile_id,
                    sum(case
                            when raw_data.datetime <= historical_prices_marked.date_1m + interval '1 week'
                                then quantity
                            end * historical_prices_marked.price_1m) as prev_close_1m
             from raw_data
                      join historical_prices_marked
                           on historical_prices_marked.symbol = raw_data.original_ticker_symbol
             where raw_data.period = '1m'
             group by profile_id
         ),
     raw_data_3m as
         (
             select profile_id,
                    sum(case
                            when raw_data.datetime <= historical_prices_marked.date_3m + interval '1 week'
                                then quantity
                            end * historical_prices_marked.price_3m) as prev_close_3m
             from raw_data
                      join historical_prices_marked
                           on historical_prices_marked.symbol = raw_data.original_ticker_symbol
             where raw_data.period = '3m'
             group by profile_id
         ),
     raw_data_1y as
         (
             select profile_id,
                    sum(case
                            when raw_data.datetime <= historical_prices_marked.date_1y + interval '1 week'
                                then quantity
                            end * historical_prices_marked.price_1y) as prev_close_1y
             from raw_data
                      join historical_prices_marked
                           on historical_prices_marked.symbol = raw_data.original_ticker_symbol
             where raw_data.period = '1y'
             group by profile_id
         ),
     raw_data_5y as
         (
             select profile_id,
                    sum(case
                            when raw_data.datetime <= historical_prices_marked.date_5y + interval '1 week'
                                then quantity
                            end * historical_prices_marked.price_5y) as prev_close_5y
             from raw_data
                      join historical_prices_marked
                           on historical_prices_marked.symbol = raw_data.original_ticker_symbol
             where raw_data.period = '5y'
             group by profile_id
         )
select *
from raw_data_1d
         left join raw_data_1w using (profile_id)
         left join raw_data_1m using (profile_id)
         left join raw_data_3m using (profile_id)
         left join raw_data_1y using (profile_id)
         left join raw_data_5y using (profile_id)