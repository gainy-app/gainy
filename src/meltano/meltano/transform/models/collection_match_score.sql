{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

select *,
       (risk_similarity > 0.3)::int + (risk_similarity > 0.7)::int         as risk_level,
       (category_similarity > 0.3)::int + (category_similarity > 0.7)::int as category_level,
       (interest_similarity > 0.3)::int + (interest_similarity > 0.7)::int as interest_level
from (
         select profile_ticker_match_score.profile_id,
                collection_id,
                collection_uniq_id,
                (sum(match_score * weight) / sum(weight))::double precision         as match_score,
                (sum(risk_similarity * weight) / sum(weight))::double precision     as risk_similarity,
                (sum(category_similarity * weight) / sum(weight))::double precision as category_similarity,
                (sum(interest_similarity * weight) / sum(weight))::double precision as interest_similarity
         from {{ source('app', 'profile_ticker_match_score') }} join {{ ref('collection_tickers_weighted') }}
         on collection_tickers_weighted.symbol = profile_ticker_match_score.symbol
             and (collection_tickers_weighted.profile_id is null or
             collection_tickers_weighted.profile_id = profile_ticker_match_score.profile_id)
         group by profile_ticker_match_score.profile_id, collection_id, collection_uniq_id
     ) t
