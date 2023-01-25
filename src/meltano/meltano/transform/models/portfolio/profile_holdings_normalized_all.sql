{{
  config(
    materialized = "incremental",
    unique_key = "holding_id_v2",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id_v2'),
      'create index if not exists "phn_profile_id_security_id_account_id" ON {{ this }} (profile_id, security_id, account_id)',
      'create index if not exists "phn_profile_id_collection_id_symbol" ON {{ this }} (profile_id, collection_id, symbol)',
      'create index if not exists "phn_profile_id_collection_uniq_id_symbol" ON {{ this }} (profile_id, collection_uniq_id, symbol)',
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
        select t.*,
               coalesce(
                   holding_since_plaid.date,
                   holding_since_dw.date
                   )::timestamp as holding_since
        from (
                 select holding_group_id,
                        holding_id_v2,
                        holding_id,
                        plaid_access_token_id,
                        security_id,
                        profile_id,
                        account_id,
                        quantity,
                        quantity_norm_for_valuation,
                        name,
                        symbol,
                        ticker_symbol,
                        collection_id,
                        collection_uniq_id,
                        type,
                        broker_uniq_id,
                        is_app_trading,
                        false as is_hidden,
                        updated_at
                 from {{ ref('profile_holdings_normalized_dynamic') }}

                 union all

                 select distinct on (
                     profile_id, portfolio_securities_normalized.original_ticker_symbol, account_id
                     ) case
                           when portfolio_securities_normalized.type = 'cash'
                               then profile_portfolio_transactions.profile_id || '_cash_' || portfolio_securities_normalized.ticker_symbol
                           else 'ticker_' || profile_portfolio_transactions.profile_id || '_' || portfolio_securities_normalized.ticker_symbol
                           end                                                           as holding_group_id,
                       profile_portfolio_transactions.profile_id ||
                       '_plaid_' || portfolio_securities_normalized.original_ticker_symbol ||
                       '_' || account_id                                                 as holding_id_v2,
                       null::int                                                         as holding_id,
                       profile_portfolio_transactions.plaid_access_token_id,
                       profile_portfolio_transactions.security_id,
                       profile_portfolio_transactions.profile_id,
                       profile_portfolio_transactions.account_id,
                       0::double precision                                               as quantity,
                       0::double precision                                               as quantity_norm_for_valuation,
                       coalesce(base_tickers.name, portfolio_securities_normalized.name) as name,
                       portfolio_securities_normalized.original_ticker_symbol            as symbol,
                       portfolio_securities_normalized.ticker_symbol,
                       null::int                                                         as collection_id,
                       null::varchar                                                     as collection_uniq_id,
                       portfolio_securities_normalized.type,
                       portfolio_brokers.uniq_id                                         as broker_uniq_id,
                       false                                                             as is_app_trading,
                       true                                                              as is_hidden,
                       greatest(profile_portfolio_transactions.updated_at,
                                portfolio_securities_normalized.updated_at,
                                base_tickers.updated_at)::timestamp                      as updated_at
                 from {{ source('app', 'profile_portfolio_transactions') }}
                          left join {{ source('app', 'profile_holdings') }} using (profile_id, security_id, account_id)
                          join {{ ref('portfolio_securities_normalized') }}
                               on portfolio_securities_normalized.id = profile_portfolio_transactions.security_id
                          left join {{ ref('base_tickers') }}
                                    on base_tickers.symbol = portfolio_securities_normalized.ticker_symbol
                          left join {{ source('app', 'profile_plaid_access_tokens') }} on profile_plaid_access_tokens.id = profile_portfolio_transactions.plaid_access_token_id
                          left join {{ ref('portfolio_brokers') }} on portfolio_brokers.plaid_institution_id = profile_plaid_access_tokens.institution_id
                 where profile_holdings.profile_id is null
             ) t
                 left join (
                        select profile_id, min(date) as date from {{ source('app', 'profile_portfolio_transactions') }} group by profile_id
                    ) holding_since_plaid using (profile_id)
                 left join (
                        select holding_id_v2, min(date) as date from {{ ref('drivewealth_portfolio_historical_holdings') }} group by holding_id_v2
                    ) holding_since_dw using(holding_id_v2)
)
select distinct on (holding_id_v2) data.*
from data

{% if is_incremental() %}
         left join {{ this }} old_data using (holding_id_v2)
where old_data.holding_id_v2 is null
   or data.updated_at > old_data.updated_at
{% endif %}
