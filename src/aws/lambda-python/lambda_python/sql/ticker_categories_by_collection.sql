with ticker_category_vectors as (
    select symbol, json_object_agg(category_id, 1.0) as ticker_category_vector
    from public.ticker_categories
    group by symbol
)
select t.symbol, tcv.ticker_category_vector
from public.tickers t
         left join ticker_category_vectors tcv
                   on t.symbol = tcv.symbol
         left join app.profile_ticker_collections as ptc
                   on t.symbol = ptc.symbol
where ptc.collection_id = %(collection_id)s and (ptc.profile_id=%(profile_id)s or ptc.profile_id is null);