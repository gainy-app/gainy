with collection_industries as (
    select tc.collection_id, ti.industry_id
    from ticker_collections tc
             join ticker_industries ti
                  on tc.symbol = ti.symbol
    where ti.industry_order = 1
)
select count(*) as corpus_size
from collection_industries;