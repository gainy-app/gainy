{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

with robinhood_options as (
    select profile_holdings.profile_id,
           sum(mod(profile_holdings.quantity::int, 100)) as quantity_module_sum
    from {{ source('app', 'profile_holdings') }}
             join {{ source('app', 'portfolio_securities') }} on portfolio_securities.id = profile_holdings.security_id
             left join {{ source('app', 'profile_plaid_access_tokens') }} on profile_plaid_access_tokens.id = profile_holdings.plaid_access_token_id
             left join {{ source('app', 'plaid_institutions') }} on plaid_institutions.id = profile_plaid_access_tokens.institution_id
    where portfolio_securities.type = 'derivative'
      and plaid_institutions.ref_id = 'ins_54'
    group by profile_holdings.profile_id
)

select profile_holdings.id                                               as holding_id,
       profile_holdings.plaid_access_token_id,
       profile_holdings.security_id,
       profile_holdings.profile_id,
       profile_holdings.account_id,
       profile_holdings.quantity / case
                                       when robinhood_options.quantity_module_sum = 0 and
                                            portfolio_securities_normalized.type = 'derivative' and
                                            plaid_institutions.ref_id = 'ins_54' then 100
                                       else 1 end                        as quantity,
       profile_holdings.quantity / case
                                       when robinhood_options.quantity_module_sum = 0 and
                                            portfolio_securities_normalized.type = 'derivative' and
                                            plaid_institutions.ref_id = 'ins_54' then 100
                                       else 1
           end * case
                     when portfolio_securities_normalized.type = 'derivative'
                         then 100
                     else 1 end                                          as quantity_norm_for_valuation, -- to multiple by price
       coalesce(base_tickers.name, portfolio_securities_normalized.name) as name,
       portfolio_securities_normalized.ticker_symbol,
       portfolio_securities_normalized.type
from {{ source('app', 'profile_holdings') }}
         join {{ ref('portfolio_securities_normalized') }}
              on portfolio_securities_normalized.id = profile_holdings.security_id
         left join {{ ref('base_tickers') }}
              on base_tickers.symbol = portfolio_securities_normalized.ticker_symbol
         left join {{ source('app', 'profile_plaid_access_tokens') }} on profile_plaid_access_tokens.id = profile_holdings.plaid_access_token_id
         left join {{ source('app', 'plaid_institutions') }} on plaid_institutions.id = profile_plaid_access_tokens.institution_id
         left join robinhood_options on robinhood_options.profile_id = profile_holdings.profile_id
where profile_holdings.quantity > 0
