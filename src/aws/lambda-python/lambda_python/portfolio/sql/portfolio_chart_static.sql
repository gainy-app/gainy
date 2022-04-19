with raw_data as
         (
             select distinct on (profile_holdings_normalized.holding_id)
                    case
                        when ticker_options.contract_name is not null
                            then 100 * profile_holdings_normalized.quantity::numeric * ticker_options.last_price::numeric
                        when portfolio_securities_normalized.type = 'cash'
                            and portfolio_securities_normalized.ticker_symbol = 'CUR:USD'
                            then profile_holdings_normalized.quantity::numeric
                        else 0
                        end as value
             from profile_holdings_normalized
                      join portfolio_securities_normalized
                           on portfolio_securities_normalized.id = profile_holdings_normalized.security_id
                      left join ticker_options
                                on ticker_options.contract_name =
                                   portfolio_securities_normalized.original_ticker_symbol
                      {join_clause}
             where true {where_clause}
         )
select sum(value) as value
from raw_data