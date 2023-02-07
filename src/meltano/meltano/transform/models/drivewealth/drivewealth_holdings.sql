{{
  config(
    materialized = "view",
  )
}}

with data as
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
                    quantity_norm_for_valuation,
                    name,
                    symbol,
                    type,
                    updated_at,
                    collection_uniq_id,
                    collection_id,
                    portfolio_status_id
             from {{ ref('drivewealth_holdings_latest') }}
         )
select distinct on (holding_id_v2) *
from data
order by holding_id_v2, portfolio_status_id desc
