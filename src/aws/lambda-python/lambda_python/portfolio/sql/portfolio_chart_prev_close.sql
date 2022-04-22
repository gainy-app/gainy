with raw_data_1d as
         (
             select distinct on (
                 uniq_id
                 ) uniq_id,
                   quantity_norm,
                   base_tickers.symbol,
                   portfolio_expanded_transactions.date as transaction_date,
                   quantity_norm::numeric *
                   historical_prices_marked.price_0d    as prev_close_1d
             from portfolio_expanded_transactions
                      join portfolio_securities_normalized
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                      join base_tickers
                           on base_tickers.symbol = portfolio_securities_normalized.original_ticker_symbol
                      join historical_prices_marked using (symbol)
                      join app.profile_portfolio_accounts on profile_portfolio_accounts.id = portfolio_expanded_transactions.account_id
                      join app.profile_plaid_access_tokens on profile_plaid_access_tokens.id = profile_portfolio_accounts.plaid_access_token_id
                 {join_clause}
                 where {where_clause}
         ),
     raw_data_1w as
         (
             select distinct on (
                 uniq_id
                 ) raw_data_1d.*,
                   quantity_norm::numeric *
                   historical_prices.adjusted_close as prev_close_1w
             from raw_data_1d
                      join historical_prices
                           on historical_prices.code = symbol
                               and historical_prices.date <= greatest(now() - interval '1 week' - interval '1 day', transaction_date)
                               and historical_prices.date >=
                                   greatest(now() - interval '1 week', transaction_date) - interval '1 week'
             order by uniq_id, historical_prices.date desc
         ),
     raw_data_1m as
         (
             select distinct on (
                 uniq_id
                 ) raw_data_1w.*,
                   quantity_norm::numeric *
                   historical_prices.adjusted_close as prev_close_1m
             from raw_data_1w
                      join historical_prices
                           on historical_prices.code = symbol
                               and historical_prices.date <= greatest(now() - interval '1 month' - interval '1 day', transaction_date)
                               and historical_prices.date >=
                                   greatest(now() - interval '1 month', transaction_date) - interval '1 week'
             order by uniq_id, historical_prices.date desc
         ),
     raw_data_3m as
         (
             select distinct on (
                 uniq_id
                 ) raw_data_1m.*,
                   quantity_norm::numeric *
                   historical_prices.adjusted_close as prev_close_3m
             from raw_data_1m
                      join historical_prices
                           on historical_prices.code = symbol
                               and historical_prices.date <= greatest(now() - interval '3 month' - interval '1 day', transaction_date)
                               and historical_prices.date >=
                                   greatest(now() - interval '3 month', transaction_date) - interval '1 week'
             order by uniq_id, historical_prices.date desc
         ),
     raw_data_1y as
         (
             select distinct on (
                 uniq_id
                 ) raw_data_3m.*,
                   quantity_norm::numeric *
                   historical_prices.adjusted_close as prev_close_1y
             from raw_data_3m
                      join historical_prices
                           on historical_prices.code = symbol
                               and historical_prices.date <= greatest(now() - interval '1 year' - interval '1 day', transaction_date)
                               and historical_prices.date >=
                                   greatest(now() - interval '1 year', transaction_date) - interval '1 week'
             order by uniq_id, historical_prices.date desc
         ),
     raw_data_5y as
         (
             select distinct on (
                 uniq_id
                 ) raw_data_1y.*,
                   quantity_norm::numeric *
                   historical_prices.adjusted_close as prev_close_5y
             from raw_data_1y
                      join historical_prices
                           on historical_prices.code = symbol
                               and historical_prices.date <= greatest(now() - interval '5 years' - interval '1 day', transaction_date)
                               and historical_prices.date >=
                                   greatest(now() - interval '5 years', transaction_date) - interval '1 week'
             order by uniq_id, historical_prices.date desc
         )
select sum(prev_close_1d)::double precision as prev_close_1d,
       sum(prev_close_1w)::double precision as prev_close_1w,
       sum(prev_close_1m)::double precision as prev_close_1m,
       sum(prev_close_3m)::double precision as prev_close_3m,
       sum(prev_close_1y)::double precision as prev_close_1y,
       sum(prev_close_5y)::double precision as prev_close_5y
from raw_data_5y
