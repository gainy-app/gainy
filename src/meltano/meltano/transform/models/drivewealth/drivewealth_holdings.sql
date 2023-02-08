{{
  config(
    materialized = "view",
  )
}}

with base_tickers_type_to_security_type as
         (
             select *
             from (
                      values ('index', 'equity'),
                             ('fund', 'equity'),
                             ('preferred stock', 'equity'),
                             ('note', 'equity'),
                             ('common stock', 'equity')
                  ) t (type, security_type)
     ),
     data as
         (
             select profile_id,
                    holding_id_v2,
                    actual_value,
                    quantity,
                    quantity_norm_for_valuation,
                    name,
                    symbol,
                    type,
                    updated_at,
                    collection_uniq_id,
                    collection_id,
                    portfolio_status_id
             from {{ ref('drivewealth_holdings_static') }}

             union all

             select profile_id,
                    holding_id_v2,
                    actual_value,
                    quantity,
                    quantity                    as quantity_norm_for_valuation,
                    base_tickers.name           as name,
                    symbol,
                    coalesce(base_tickers_type_to_security_type.security_type,
                             base_tickers.type) as type,
                    drivewealth_portfolio_holdings.updated_at,
                    collection_uniq_id,
                    collection_id,
                    portfolio_status_id
             from {{ source('app', 'drivewealth_portfolio_holdings') }}
                      left join {{ ref('base_tickers') }} using (symbol)
                      left join base_tickers_type_to_security_type using (type)
         )
select distinct on (holding_id_v2) *
from data
order by holding_id_v2, portfolio_status_id desc
