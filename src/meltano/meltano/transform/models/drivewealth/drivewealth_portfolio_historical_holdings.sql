{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('profile_id, collection_id, symbol, date'),
      index('id', true),
      'create index if not exists "dphh_profile_id_collection_id_symbol_date_week" ON {{ this }} (profile_id, collection_id, symbol, date_week)',
      'create index if not exists "dphh_profile_id_collection_id_symbol_date_month" ON {{ this }} (profile_id, collection_id, symbol, date_month)',
    ]
  )
}}

with portfolio_statuses as
         (
             select distinct on (profile_id, date) *
             from (
                      select profile_id,
                             drivewealth_portfolio_statuses.created_at                               as updated_at,
                             (drivewealth_portfolio_statuses.created_at - interval '20 hours')::date as date,
                             cash_value / drivewealth_portfolio_statuses.cash_actual_weight          as value,
                             drivewealth_portfolio_statuses.data -> 'holdings'                       as holdings
                      from {{ source('app', 'drivewealth_portfolio_statuses')}}
                               join {{ source('app', 'drivewealth_portfolios')}}
                                    on drivewealth_portfolios.ref_id = drivewealth_portfolio_id
                  ) t
             order by profile_id, date desc, updated_at desc
         ),
     portfolio_status_funds as
         (
             select profile_id,
                    date,
                    value,
                    updated_at,
                    json_array_elements(holdings) as portfolio_holding_data
             from portfolio_statuses
     ),
     fund_holdings as
         (
             select portfolio_status_funds.profile_id,
                    portfolio_status_funds.date,
                    drivewealth_funds.collection_id,
                    portfolio_status_funds.updated_at,
                    portfolio_holding_data,
                    json_array_elements(portfolio_holding_data -> 'holdings') as fund_holding_data
             from portfolio_status_funds
                      join {{ source('app', 'drivewealth_funds')}}
                           on drivewealth_funds.ref_id = portfolio_holding_data ->> 'id'
             where portfolio_holding_data ->> 'type' != 'CASH_RESERVE'
     ),
     data as
         (
             select profile_id,
                    collection_id,
                    (fund_holding_data ->> 'symbol')           as symbol,
                    date,
                    (fund_holding_data ->> 'value')::numeric   as value,
                    updated_at
             from fund_holdings
     )
select data.*,
       date_trunc('week', date)::date                                     as date_week,
       date_trunc('month', date)::date                                    as date_month,
       profile_id || '_' || collection_id || '_' || symbol || '_' || date as id
from data

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, collection_id, symbol, date)
where old_data.profile_id is null
   or data.updated_at > old_data.updated_at
{% endif %}
