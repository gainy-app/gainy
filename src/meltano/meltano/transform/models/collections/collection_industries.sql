{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

with inverted_industry_frequency as (
	select
        industry_id,
        symbol,
        1.0 / count(*) over (partition by symbol) as inverted_industry_frequency
	from {{ ref('ticker_industries') }}
)
select
       c.id as collection_id,
       iif.industry_id,
       sum(iif.inverted_industry_frequency)::real / c.size as industry_grade
from {{ ref('collections') }} c
         join {{ ref('collection_ticker_actual_weights') }}
              on collection_ticker_actual_weights.collection_id = c.id
         join inverted_industry_frequency iif
              on iif.symbol = collection_ticker_actual_weights.symbol
where c.enabled = '1'
group by c.id, iif.industry_id, c.size