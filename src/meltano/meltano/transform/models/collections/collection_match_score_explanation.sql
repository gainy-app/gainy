{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


with t_cat_sim_dif as
         (
             select category_id, symbol, sim_dif / 2 + 0.5 as sim_dif
             from {{ ref('ticker_categories_continuous') }}
         ),
     t_int_sim_dif as
         (
             select interest_id, symbol, sim_dif / 2 + 0.5 as sim_dif
             from {{ ref('ticker_interests') }}
         ),
     profile_collection_categories as
         (
             select t.profile_id,
                    t.collection_id,
                    t.collection_uniq_id,
                    t.category_id,
                    t.match_score * weight as match_score
             from (
                      select profile_ticker_match_score.profile_id,
                             collection_tickers_weighted.collection_id,
                             collection_tickers_weighted.collection_uniq_id,
                             t_cat_sim_dif.category_id,
                             sum(match_score * sim_dif) as match_score
                      from {{ source('app', 'profile_ticker_match_score')}}
                               join {{ ref('collection_tickers_weighted') }}
                                    on collection_tickers_weighted.symbol = profile_ticker_match_score.symbol
                                        and (collection_tickers_weighted.profile_id is null or
                                             collection_tickers_weighted.profile_id =
                                             profile_ticker_match_score.profile_id)
                               join t_cat_sim_dif on t_cat_sim_dif.symbol = profile_ticker_match_score.symbol
                      group by profile_ticker_match_score.profile_id,
                               collection_tickers_weighted.collection_id,
                               collection_tickers_weighted.collection_uniq_id,
                               category_id
                  ) t
                      join {{ ref('collection_piechart') }}
                           on collection_piechart.collection_id = t.collection_id
                               and (collection_piechart.profile_id = t.profile_id or
                                    collection_piechart.profile_id is null)
                               and collection_piechart.entity_type = 'category'
                               and collection_piechart.entity_id = category_id::varchar
         ),
     profile_collection_interests as
         (
             select t.profile_id,
                    t.collection_id,
                    t.collection_uniq_id,
                    t.interest_id,
                    t.match_score * weight as match_score
             from (
                      select profile_ticker_match_score.profile_id,
                             collection_tickers_weighted.collection_id,
                             collection_tickers_weighted.collection_uniq_id,
                             t_int_sim_dif.interest_id,
                             sum(match_score * sim_dif) as match_score
                      from {{ source('app', 'profile_ticker_match_score')}}
                               join {{ ref('collection_tickers_weighted') }}
                                    on collection_tickers_weighted.symbol = profile_ticker_match_score.symbol
                                        and (collection_tickers_weighted.profile_id is null or
                                             collection_tickers_weighted.profile_id =
                                             profile_ticker_match_score.profile_id)
                               join t_int_sim_dif on t_int_sim_dif.symbol = profile_ticker_match_score.symbol
                      group by profile_ticker_match_score.profile_id,
                               collection_tickers_weighted.collection_id,
                               collection_tickers_weighted.collection_uniq_id,
                               interest_id
                  ) t
                      join {{ ref('collection_piechart') }}
                           on collection_piechart.collection_id = t.collection_id
                               and (collection_piechart.profile_id = t.profile_id or
                                    collection_piechart.profile_id is null)
                               and collection_piechart.entity_type = 'interest'
                               and collection_piechart.entity_id = interest_id::varchar
--              where weight > 0.2
         ),
     profile_collection_matches as
         (
             select t.*,
                    user_id
             from (
                      select profile_id,
                             collection_id,
                             collection_uniq_id,
                             match_score,
                             category_id,
                             null as interest_id
                      from profile_collection_categories
                      union all
                      select profile_id,
                             collection_id,
                             collection_uniq_id,
                             match_score,
                             null as category_id,
                             interest_id
                      from profile_collection_interests
                  ) t
                      join {{ source('app', 'profiles')}} on profiles.id = t.profile_id
         ),
     profile_collection_matches_ranked as
         (
             select profile_collection_matches.*,
                    row_number() over (partition by profile_id, collection_id order by match_score desc) as row_num
             from profile_collection_matches
         )
select profile_id,
       collection_id,
       collection_uniq_id,
       category_id,
       interest_id,
       user_id
from profile_collection_matches_ranked
where row_num <= 5
