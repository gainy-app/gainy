{{
  config(
    materialized = "view"
  )
}}

with inverted_category_frequency as (
	select
        category_id,
        symbol, 1.0 / count(*) over (partition by symbol) as inverted_category_frequency
	from {{ ref('ticker_categories') }}
)
select c.id as collection_id, itf.category_id, sum(itf.inverted_category_frequency)::real / c.size as category_grade
from {{ ref('collections') }} c
        join {{ ref('ticker_collections') }} tc
            on c.id = tc.collection_id
        join inverted_category_frequency itf
            on tc.symbol = itf.symbol
where c.enabled = '1'
group by c.id, itf.category_id, c.size