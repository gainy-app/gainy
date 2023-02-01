{{
  config(
    materialized = "view",
  )
}}

select holding_group_id                                            as id,
       profile_id,
       min(case when collection_id is null then ticker_symbol end) as symbol,
       min(collection_id)                                          as collection_id,
       min(collection_uniq_id)                                     as collection_uniq_id,
       sum(profile_holdings_normalized_dynamic.quantity)           as quantity -- deprecated
from {{ ref('profile_holdings_normalized_dynamic') }}
group by holding_group_id, profile_id
