{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

with positions0 as
         (
             select distinct on (
                 profile_id,
                 drivewealth_accounts_positions.created_at::date
                 ) profile_id,
                   drivewealth_accounts_positions.created_at::date as date,
                   drivewealth_accounts_positions.*
             from {{ source('app', 'drivewealth_accounts_positions') }}
                      join {{ source('app', 'drivewealth_accounts') }}
                           on drivewealth_accounts.ref_id = drivewealth_accounts_positions.drivewealth_account_id
                      join {{ source('app', 'drivewealth_users') }}
                           on drivewealth_users.ref_id = drivewealth_accounts.drivewealth_user_id
             order by profile_id, drivewealth_accounts_positions.created_at::date, created_at desc
         ),
     positions1 as
         (
             select profile_id,
                    date,
                    created_at                                     as updated_at,
                    json_array_elements(data -> 'equityPositions') as position_data
             from positions0
     )
select profile_id,
       position_data ->> 'symbol'                          as symbol,
       positions1.date,
       (position_data ->> 'openQty')::double precision     as quantity,
       (position_data ->> 'mktPrice')::double precision    as price,
       (position_data ->> 'marketValue')::double precision as value,
       (position_data ->> 'costBasis')::double precision   as cost,
       updated_at
from positions1
