with raw_data_1d as
         (
             select distinct on (
                 uniq_id
                 ) uniq_id,
                   quantity_norm_for_valuation                                     as quantity_norm,
                   portfolio_securities_normalized.original_ticker_symbol          as symbol,
                   portfolio_expanded_transactions.date                            as transaction_date,
                   quantity_norm_for_valuation * historical_prices_marked.price_0d as prev_close_1d
             from portfolio_expanded_transactions
                      join portfolio_securities_normalized
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                      join historical_prices_marked
                           on historical_prices_marked.symbol = portfolio_securities_normalized.original_ticker_symbol
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
                   case when historical_prices.date >= transaction_date then quantity_norm end *
                   historical_prices.adjusted_close as prev_close_1w
             from raw_data_1d
                      left join historical_prices
                                on historical_prices.code = symbol
                                    and historical_prices.date <= now() - interval '1 week' - interval '1 day'
                                    and historical_prices.date >= now() - interval '1 week' - interval '1 week'
             order by uniq_id, historical_prices.date desc
         ),
     raw_data_1m as
         (
             select distinct on (
                 uniq_id
                 ) raw_data_1w.*,
                   case when historical_prices.date >= transaction_date then quantity_norm end *
                   historical_prices.adjusted_close as prev_close_1m
             from raw_data_1w
                      left join historical_prices
                                on historical_prices.code = symbol
                                    and historical_prices.date <= now() - interval '1 month' - interval '1 day'
                                    and historical_prices.date >= now() - interval '1 month' - interval '1 week'
             order by uniq_id, historical_prices.date desc
         ),
     raw_data_3m as
         (
             select distinct on (
                 uniq_id
                 ) raw_data_1m.*,
                   case when historical_prices.date >= transaction_date then quantity_norm end *
                   historical_prices.adjusted_close as prev_close_3m
             from raw_data_1m
                      left join historical_prices
                                on historical_prices.code = symbol
                                    and historical_prices.date <= now() - interval '3 month' - interval '1 day'
                                    and historical_prices.date >= now() - interval '3 month' - interval '1 week'
             order by uniq_id, historical_prices.date desc
         ),
     raw_data_1y as
         (
             select distinct on (
                 uniq_id
                 ) raw_data_3m.*,
                   case when historical_prices.date >= transaction_date then quantity_norm end *
                   historical_prices.adjusted_close as prev_close_1y
             from raw_data_3m
                      left join historical_prices
                                on historical_prices.code = symbol
                                    and historical_prices.date <= now() - interval '1 year' - interval '1 day'
                                    and historical_prices.date >= now() - interval '1 year' - interval '1 week'
             order by uniq_id, historical_prices.date desc
         ),
     raw_data_5y as
         (
             select distinct on (
                 uniq_id
                 ) raw_data_1y.*,
                   case when historical_prices.date >= transaction_date then quantity_norm end *
                   historical_prices.adjusted_close as prev_close_5y
             from raw_data_1y
                      left join historical_prices
                                on historical_prices.code = symbol
                                    and historical_prices.date <= now() - interval '5 years' - interval '1 day'
                                    and historical_prices.date >= now() - interval '5 years' - interval '1 week'
             order by uniq_id, historical_prices.date desc
         )
select sum(prev_close_1d)::double precision as prev_close_1d,
       sum(prev_close_1w)::double precision as prev_close_1w,
       sum(prev_close_1m)::double precision as prev_close_1m,
       sum(prev_close_3m)::double precision as prev_close_3m,
       sum(prev_close_1y)::double precision as prev_close_1y,
       sum(prev_close_5y)::double precision as prev_close_5y
from raw_data_5y
