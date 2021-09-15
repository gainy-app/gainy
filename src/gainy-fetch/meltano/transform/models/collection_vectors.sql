{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'collection_id', true),
      fk(this, 'collection_id', 'collections', 'id')
    ]
  )
}}

with collection_industries as (
    select
        tc.collection_id,
        ti.industry_id
    from
        {{ ref('ticker_collections') }} tc
    join
        ticker_industries ti
    on
        tc.symbol = ti.symbol
),
collection_norm as (
    select
        collection_id,
        sqrt(count(*))::real as industry_norm
    from
        collection_industries
    group by
        collection_id
     ),
collection_industry_scores as (
    select
        collection_id,
        industry_id,
        count(*)::real as industry_score
    from
        collection_industries
    group by
        collection_id,
        industry_id
)
select
    cis.collection_id,
    json_object_agg(cis.industry_id, cis.industry_score / cn.industry_norm) as industry_score_json
from
    collection_industry_scores cis
join
    collection_norm cn
on
    cis.collection_id = cn.collection_id
group by
    cis.collection_id