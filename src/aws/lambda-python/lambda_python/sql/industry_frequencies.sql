with collection_industries as (
    select tc.collection_id, ti.industry_id
    from ticker_collections tc
             join ticker_industries ti
                  on tc.symbol = ti.symbol
)
select ci.industry_id, count(*) as frequency
from app.profile_collections c
         left join collection_industries ci
                   on c.id = ci.collection_id
where c.enabled = '1' and c.personalized='0'
group by ci.industry_id;