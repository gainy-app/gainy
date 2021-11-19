{{
  config(
    materialized = "view"
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
         join {{ ref('ticker_collections') }} tc
              on c.id = tc.collection_id
         join inverted_industry_frequency iif
              on tc.symbol = iif.symbol
where c.enabled = '1'
group by c.id, iif.industry_id, c.size