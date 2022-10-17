{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

select holding_group_id as id,
       profile_id,
       case when collection_id is null then ticker_symbol end                                   as symbol,
       collection_id,
       sum(profile_holdings_normalized.quantity)       as quantity
from {{ ref('profile_holdings_normalized') }}
group by holding_group_id, profile_id, ticker_symbol, collection_id
