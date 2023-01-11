{{
  config(
    materialized = "view",
  )
}}

with latest_portfolio_status as
         (
             select distinct on (
                 drivewealth_portfolio_id
                 ) profile_id,
                   drivewealth_portfolio_statuses.*
             from {{ source('app', 'drivewealth_portfolio_statuses') }}
                      join {{ source('app', 'drivewealth_portfolios') }}
                           on drivewealth_portfolios.ref_id = drivewealth_portfolio_id
             order by drivewealth_portfolio_id, created_at desc
         ),
     portfolio_funds as
         (
             select profile_id,
                    created_at                              as updated_at,
                    json_array_elements(data -> 'holdings') as portfolio_holding_data
             from latest_portfolio_status
     ),
     fund_holdings as
         (
             select portfolio_funds.profile_id,
                    drivewealth_funds.collection_id,
                    greatest(portfolio_funds.updated_at,
                             drivewealth_funds.updated_at)                    as updated_at,
                    json_array_elements(portfolio_holding_data -> 'holdings') as fund_holding_data
             from portfolio_funds
                      join {{ source('app', 'drivewealth_funds') }} on drivewealth_funds.ref_id = portfolio_holding_data ->> 'id'
             where portfolio_holding_data ->> 'type' != 'CASH_RESERVE'
     ),
     fund_holdings_distinct as
         (
             select profile_id,
                    collection_id,
                    fund_holding_data ->> 'symbol'                           as symbol,
                    sum((fund_holding_data ->> 'openQty')::double precision) as quantity,
                    max(updated_at)                                          as updated_at
             from fund_holdings
             group by profile_id, collection_id, fund_holding_data ->> 'symbol'
     ),
     base_tickers_type_to_security_type as
         (
             select *
             from (
                      values ('index', 'equity'),
                             ('fund', 'equity'),
                             ('preferred stock', 'equity'),
                             ('note', 'equity'),
                             ('common stock', 'equity')
                  ) t (type, security_type)
     )
select fund_holdings_distinct.profile_id,
       fund_holdings_distinct.quantity                as quantity,
       fund_holdings_distinct.quantity                as quantity_norm_for_valuation,
       fund_holdings_distinct.quantity * actual_price as actual_value,
       base_tickers.name                              as name,
       symbol,
       coalesce(base_tickers_type_to_security_type.security_type,
                base_tickers.type)                    as type,
       null::timestamp                                as purchase_date,
       greatest(fund_holdings_distinct.updated_at,
                base_tickers.updated_at)              as updated_at,
       profile_collections.uniq_id                    as collection_uniq_id,
       fund_holdings_distinct.collection_id
from fund_holdings_distinct
         left join {{ ref('base_tickers') }} using (symbol)
         left join base_tickers_type_to_security_type using (type)
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
         left join {{ ref('profile_collections') }}
                   on profile_collections.id = fund_holdings_distinct.collection_id
                       and (profile_collections.profile_id = fund_holdings_distinct.profile_id or profile_collections.profile_id is null)

union all

select profile_id,
       sum((portfolio_holding_data ->> 'value')::double precision) as quantity,
       sum((portfolio_holding_data ->> 'value')::double precision) as quantity_norm_for_valuation,
       sum((portfolio_holding_data ->> 'value')::double precision) as actual_value,
       'U S Dollar'                                                as name,
       'CUR:USD'                                                   as symbol,
       'cash'                                                      as type,
       null::timestamp                                             as purchase_date,
       max(updated_at)                                             as updated_at,
       null                                                        as collection_uniq_id,
       null                                                        as collection_id
from portfolio_funds
where portfolio_holding_data ->> 'type' = 'CASH_RESERVE'
group by profile_id
