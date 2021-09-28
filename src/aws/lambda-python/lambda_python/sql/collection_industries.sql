with collection_industries as (
    select tc.collection_id, ti.industry_id
    from ticker_collections tc
    join ticker_industries ti
    on tc.symbol = ti.symbol
),
collection_industry_scores as (
    select collection_id, industry_id, count(*)::real as industry_count
    from collection_industries
    group by collection_id, industry_id
),
collection_industry_vectors as (
    select cis.collection_id, json_object_agg(cis.industry_id, cis.industry_count) as collection_industry_vector
    from collection_industry_scores cis
    group by cis.collection_id
)
select c.id, civ.collection_industry_vector
from public.collections c
left join collection_industry_vectors civ
on c.id = civ.collection_id
where c.enabled = '1';