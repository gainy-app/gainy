with collection_ticker_match_score as (
    select profile_id, collection_id, match_score
    from ticker_collections tc
             join app.profile_ticker_match_score ptm
                  on tc.symbol = ptm.symbol
),
     collection_ranking_score as (
         select profile_id, collection_id, avg(match_score) as ranking_score
         from collection_ticker_match_score
         where profile_id = %(profile_id)s
         group by profile_id, collection_id
     )
select c.id as collection_id, coalesce(crs.ranking_score, 0.0) as ranking_score
from collections c
         left join collection_ranking_score crs
                   on c.id = crs.collection_id
where c.enabled = '1' and c.personalized = '0'
order by ranking_score desc;