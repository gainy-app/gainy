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
                   greatest(drivewealth_portfolio_statuses.created_at,
                            drivewealth_portfolios.updated_at) as updated_at,
                   drivewealth_portfolio_statuses.*
             from {{ source('app', 'drivewealth_portfolio_statuses') }}
                      join {{ source('app', 'drivewealth_portfolios') }}
                           on drivewealth_portfolios.ref_id = drivewealth_portfolio_id
             order by profile_id, created_at desc
         ),
     portfolio_funds as
         (
             select profile_id,
                    updated_at,
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
select profile_id,
       null                                                          as account_id,
       (fund_holding_data ->> 'openQty')::numeric                    as quantity,
       (fund_holding_data ->> 'openQty')::numeric                    as quantity_norm_for_valuation,
       (fund_holding_data ->> 'openQty')::numeric *
       ticker_realtime_metrics.actual_price                          as actual_value,
       base_tickers.name                                             as name,
       base_tickers.symbol                                           as symbol,
       coalesce(base_tickers_type_to_security_type.security_type,
                base_tickers.type)                                   as type,
       null::timestamp                                               as purchase_date,
       greatest(fund_holdings.updated_at,
                base_tickers.updated_at)                             as updated_at,
       collection_id
from fund_holdings
         left join {{ ref('base_tickers') }}
                   on base_tickers.symbol = fund_holding_data ->> 'symbol'
         left join base_tickers_type_to_security_type using (type)
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)

union all

select portfolio_funds.profile_id,
       null                                          as account_id,
       (portfolio_holding_data ->> 'value')::numeric as quantity,
       (portfolio_holding_data ->> 'value')::numeric as quantity_norm_for_valuation,
       (portfolio_holding_data ->> 'value')::numeric as actual_value,
       'U S Dollar'                                  as name,
       'CUR:USD'                                     as symbol,
       'cash'                                        as type,
       null::timestamp                               as purchase_date,
       updated_at,
       null                                          as collection_id
from portfolio_funds
where portfolio_holding_data ->> 'type' = 'CASH_RESERVE'
