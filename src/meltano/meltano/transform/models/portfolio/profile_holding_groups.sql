{{
  config(
    materialized = "view",
  )
}}

select holding_group_id                          as id,
       profile_id,
       ticker_symbol                             as symbol,
       null::int                                 as collection_id,
       null::varchar                             as collection_uniq_id,
       sum(profile_holdings_normalized.quantity) as quantity
from {{ ref('profile_holdings_normalized') }}
where collection_id is null
group by holding_group_id, profile_id, ticker_symbol

union all

select holding_group_id       as id,
       profile_id,
       null::varchar          as symbol,
       collection_id,
       collection_uniq_id,
       null::double precision as quantity
from {{ ref('profile_holdings_normalized') }}
where collection_id is not null
group by holding_group_id, profile_id, collection_uniq_id, collection_id
