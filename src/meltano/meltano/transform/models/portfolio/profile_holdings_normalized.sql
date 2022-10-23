{{
  config(
    materialized = "incremental",
    unique_key = "holding_id_v2",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id_v2'),
      'create index if not exists "phn_profile_id_security_id_account_id" ON {{ this }} (profile_id, security_id, account_id)',
      'create index if not exists "phn_profile_id_collection_id_symbol" ON {{ this }} (profile_id, collection_id, symbol)',
      fk('holding_id', 'app', 'profile_holdings', 'id'),
      fk('plaid_access_token_id', 'app', 'profile_plaid_access_tokens', 'id'),
      fk('security_id', this.schema, 'portfolio_securities_normalized', 'id'),
      fk('profile_id', 'app', 'profiles', 'id'),
      fk('account_id', 'app', 'profile_portfolio_accounts', 'id'),
    ]
  )
}}


with data as
    (
        select case
                   when portfolio_securities_normalized.type = 'cash'
                       then profile_holdings.profile_id || '_cash_' || portfolio_securities_normalized.ticker_symbol
                   else 'ticker_' || profile_holdings.profile_id || '_' || portfolio_securities_normalized.ticker_symbol
                   end                                                           as holding_group_id,
               profile_holdings.profile_id ||
               '_plaid_' || profile_holdings.id                                  as holding_id_v2,
               profile_holdings.id                                               as holding_id,
               profile_holdings.plaid_access_token_id,
               profile_holdings.security_id,
               profile_holdings.profile_id,
               profile_holdings.account_id,
               profile_holdings.quantity                                         as quantity,
               profile_holdings.quantity * case
                             when portfolio_securities_normalized.type = 'derivative'
                                 then 100
                             else 1 end                                          as quantity_norm_for_valuation, -- to multiple by price
               coalesce(base_tickers.name, portfolio_securities_normalized.name) as name,
               portfolio_securities_normalized.original_ticker_symbol            as symbol,
               portfolio_securities_normalized.ticker_symbol,
               null                                                              as collection_id,
               portfolio_securities_normalized.type,
               portfolio_brokers.uniq_id                                         as broker_uniq_id,
               greatest(profile_holdings.updated_at,
                        portfolio_securities_normalized.updated_at,
                        base_tickers.updated_at)                                 as updated_at
        from {{ source('app', 'profile_holdings') }}
                 join {{ ref('portfolio_securities_normalized') }}
                      on portfolio_securities_normalized.id = profile_holdings.security_id
                 left join {{ ref('base_tickers') }}
                           on base_tickers.symbol = portfolio_securities_normalized.ticker_symbol
                 left join {{ source('app', 'profile_plaid_access_tokens') }} on profile_plaid_access_tokens.id = profile_holdings.plaid_access_token_id
                 left join {{ ref('portfolio_brokers') }} on portfolio_brokers.plaid_institution_id = profile_plaid_access_tokens.institution_id
        where profile_holdings.quantity > 0

        union all

        select case
                   when drivewealth_holdings.type = 'cash'
                       then profile_id || '_cash_' || symbol
                   else 'ttf_' || profile_id || coalesce('_' || collection_id, '')
                   end                                                                     as holding_group_id,
               'ttf_' || profile_id || coalesce('_' || collection_id, '') || '_' || symbol as holding_id_v2,
               null                                                                        as holding_id,
               null                                                                        as plaid_access_token_id,
               null                                                                        as security_id,
               profile_id,
               null                                                                        as account_id,
               quantity,
               quantity_norm_for_valuation,
               coalesce(base_tickers.name, drivewealth_holdings.name)                      as name,
               symbol,
               symbol                                                                      as ticker_symbol,
               collection_id,
               drivewealth_holdings.type,
               portfolio_brokers.uniq_id                                                   as broker_uniq_id,
               greatest(drivewealth_holdings.updated_at, base_tickers.updated_at)          as updated_at
        from {{ ref('drivewealth_holdings') }}
                 left join {{ ref('base_tickers') }} using (symbol)
                 left join {{ ref('portfolio_brokers') }} on portfolio_brokers.uniq_id = 'dw_ttf'
)
select data.*
from data

{% if is_incremental() %}
         left join {{ this }} old_data using (holding_id_v2)
where old_data.holding_id_v2 is null
   or data.updated_at > old_data.updated_at
{% endif %}
