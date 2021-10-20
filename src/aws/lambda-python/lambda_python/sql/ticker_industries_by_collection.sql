with ticker_industry_vectors as (
    select symbol, json_object_agg(industry_id, 1.0) as ticker_industry_vector
    from public.ticker_industries
    group by symbol
)
select t.symbol, tiv.ticker_industry_vector
from public.tickers t
         left join ticker_industry_vectors tiv
                   on t.symbol = tiv.symbol
         left join public.profile_ticker_collections as ptc
                   on t.symbol = ptc.symbol
where ptc.collection_id = %(collection_id)s and (ptc.profile_id=%(profile_id)s or ptc.profile_id is null);