{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

select profile_id,
       collection_id,
       collection_uniq_id,
       datetime,
       period,
       sum(open * weight)           as open,
       sum(high * weight)           as high,
       sum(low * weight)            as low,
       sum(close * weight)          as close,
       sum(adjusted_close * weight) as adjusted_close
from {{ ref('collection_tickers_weighted') }}
         join {{ ref('chart') }} using (symbol)
group by profile_id, collection_id, collection_uniq_id, datetime, period
