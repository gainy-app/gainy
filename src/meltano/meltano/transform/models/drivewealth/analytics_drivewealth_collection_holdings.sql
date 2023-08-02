{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

with latest_portfolio_status as
         (
             select drivewealth_portfolio_statuses.*
             from {{ source('app', 'drivewealth_portfolio_statuses') }}
                      join (
                               select max(id) as id
                               from {{ source('app', 'drivewealth_portfolio_statuses') }}
                               group by drivewealth_portfolio_id, date
                           ) t using (id)
         ),
     portfolio_funds as
         (
             select latest_portfolio_status.date,
                    created_at                              as updated_at,
                    json_array_elements(data -> 'holdings') as portfolio_holding_data
             from latest_portfolio_status
     )
select profile_id,
       collection_id,
       portfolio_funds.date,
       max(portfolio_funds.updated_at)                             as updated_at,
       sum((portfolio_holding_data ->> 'value')::double precision) as value
from portfolio_funds
         join {{ source('app', 'drivewealth_funds') }} on drivewealth_funds.ref_id = portfolio_holding_data ->> 'id'
where portfolio_holding_data ->> 'type' != 'CASH_RESERVE'
  and collection_id is not null
group by profile_id, collection_id, portfolio_funds.date
