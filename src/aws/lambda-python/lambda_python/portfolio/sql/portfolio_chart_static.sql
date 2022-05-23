with raw_data as
         (
             select distinct on (profile_holdings_normalized.holding_id)
                    case
                        when portfolio_securities_normalized.type = 'cash'
                            and portfolio_securities_normalized.ticker_symbol = 'CUR:USD'
                            then profile_holdings_normalized.quantity::numeric
                        else 0
                        end as value
             from profile_holdings_normalized
                      join portfolio_securities_normalized
                           on portfolio_securities_normalized.id = profile_holdings_normalized.security_id
                      join app.profile_portfolio_accounts on profile_portfolio_accounts.id = profile_holdings_normalized.account_id
                      join app.profile_plaid_access_tokens on profile_plaid_access_tokens.id = profile_portfolio_accounts.plaid_access_token_id
                      {join_clause}
             where portfolio_securities_normalized.type = 'cash'
               and portfolio_securities_normalized.ticker_symbol = 'CUR:USD'
               and ({where_clause})
         )
select sum(value) as value
from raw_data
