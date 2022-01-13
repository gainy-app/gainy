with collection_ticker_match_score as (
    select profile_id, collection_id, match_score
    from ticker_collections tc
             join app.profile_ticker_match_view ptm
                  on tc.symbol = ptm.symbol
),
     collection_ranking_score as (
         select profile_id, collection_id, avg(match_score) as ranking_score
         from collection_ticker_match_score
         group by profile_id, collection_id
     )
select crs.collection_id, crs.ranking_score
from collection_ranking_score crs
where crs.profile_id = %(profile_id)s
order by crs.ranking_score desc;