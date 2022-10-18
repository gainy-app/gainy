{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

with latest_portfolio_status as
         (
             select distinct on (
                 profile_id
                 ) profile_id,
                   drivewealth_portfolio_statuses.*
             from {{ source('app', 'drivewealth_portfolio_statuses') }}
                      join {{ source('app', 'drivewealth_portfolios') }} on drivewealth_portfolios.ref_id = drivewealth_portfolio_id
             order by profile_id, created_at desc
         ),
     portfolio_funds as
         (
             select profile_id,
                    json_array_elements(data -> 'holdings') as portfolio_holding_data
             from latest_portfolio_status
     ),
     fund_holdings as
         (
             select portfolio_funds.profile_id,
                    drivewealth_funds.collection_id,
                    json_array_elements(portfolio_holding_data -> 'holdings') as fund_holding_data
             from portfolio_funds
                      join {{ source('app', 'drivewealth_funds') }} on drivewealth_funds.ref_id = portfolio_holding_data ->> 'id'
             where portfolio_holding_data ->> 'type' != 'CASH_RESERVE'
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
select 'ttf_' || profile_id || '_' || collection_id                                  as holding_group_id,
       'ttf_' || profile_id || '_' || collection_id || '_' || symbol                 as holding_id,
       profile_id,
       null                                                                          as account_id,
       (fund_holding_data ->> 'openQty')::numeric                                    as quantity,
       (fund_holding_data ->> 'openQty')::numeric                                    as quantity_norm_for_valuation,
       (fund_holding_data ->> 'value')::numeric                                      as actual_value,
       base_tickers.name                                                             as name,
       base_tickers.symbol                                                           as symbol,
       coalesce(base_tickers_type_to_security_type.security_type, base_tickers.type) as type,
       null::timestamp                                                               as purchase_date,
       null::timestamp                                                               as updated_at,
       collection_id
from fund_holdings
         left join {{ ref('base_tickers') }}
                   on base_tickers.symbol = fund_holding_data ->> 'symbol'
         left join base_tickers_type_to_security_type using (type)

union all

select null                                          as holding_group_id,
       null                                          as holding_id,
       portfolio_funds.profile_id,
       null                                          as account_id,
       (portfolio_holding_data ->> 'value')::numeric as quantity,
       (portfolio_holding_data ->> 'value')::numeric as quantity_norm_for_valuation,
       (portfolio_holding_data ->> 'value')::numeric as actual_value,
       'U S Dollar'                                  as name,
       'CUR:USD'                                     as symbol,
       'cash'                                        as type,
       null::timestamp                               as purchase_date,
       now()                                         as updated_at,
       null                                          as collection_id
from portfolio_funds
where portfolio_holding_data ->> 'type' = 'CASH_RESERVE'
