with ticker_category_vectors as (
    select symbol, json_object_agg(category_id, 1.0) as ticker_category_vector
    from public.ticker_categories
    group by symbol
)
select t.symbol, tcv.ticker_category_vector
from public.tickers t
         left join ticker_category_vectors tcv
                   on t.symbol = tcv.symbol
         left join public.ticker_collections as tc
                   on t.symbol = tc.symbol
where tc.collection_id = %(collection_id)s;