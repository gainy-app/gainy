{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


with p_rsk as
         (
             select profile_id, (risk_score::double precision - 1) / 2 as value
             from {{ source('app', 'profile_scoring_settings') }}
         ),
     p_cat as
         (
             select profile_id, category_id
             from {{ source('app', 'profile_categories') }}
         ),
     p_int as
         (
             select profile_id, interest_id
             from {{ source('app', 'profile_interests') }}
         ),
     t_cat_sim_dif as
         (
             select category_id, symbol, sim_dif
             from {{ ref('ticker_categories_continuous') }}
         ),
     t_int_sim_dif as
         (
             select interest_id, symbol, sim_dif
             from {{ ref('ticker_interests') }}
         ),
     t_risk_score as
         (
             select symbol, risk_score
             from {{ ref('ticker_risk_scores') }}
         ),
     const as
         (
             select *
             from (values (3.8, 6.53, 3.38)) t ("d", "sr", "sc")
         ),
     risk_similarity as
         (
             select profile_id,
                    symbol,
                    1. / (1. + pow(abs(p_rsk.value - coalesce(t_risk_score.risk_score, 0.5)), d) *
                               pow(abs(sr + (sc - sr) * abs(p_rsk.value - 0.5) / 0.5), d)) * 2 - 1 as match_comp_risk
             from {{ source('app', 'profiles') }}
                      join const on true
                      left join p_rsk on p_rsk.profile_id = profiles.id
                      left join {{ ref('tickers') }} on true
                      left join t_risk_score using (symbol)
         ),
     category_similarity as
         (
             select profile_id,
                    symbol,
                    coalesce(max(sim_dif), -1) as match_comp_category
             from {{ source('app', 'profiles') }}
                      left join p_cat on p_cat.profile_id = profiles.id
                      left join {{ ref('tickers') }} on true
                      left join t_cat_sim_dif using (category_id, symbol)
             group by profile_id, symbol
         ),
     category_matches as
         (
             select profile_id,
                    symbol,
                    json_agg(category_id) as category_matches
             from (
                      select profile_id,
                             symbol,
                             category_id,
                             sim_dif,
                             row_number()
                             over (partition by profile_id, symbol order by sim_dif desc nulls last) as row_number
                      from {{ source('app', 'profiles') }}
                               left join p_cat on p_cat.profile_id = profiles.id
                               left join {{ ref('tickers') }} on true
                               left join t_cat_sim_dif using (category_id, symbol)
                      order by profile_id, symbol, sim_dif desc nulls last
                  ) t
             where row_number <= 2
               and sim_dif is not null
               and sim_dif > 0
             group by profile_id, symbol
         ),
     interest_similarity as
         (
             select profile_id,
                    symbol,
                    coalesce(max(sim_dif), -1) as match_comp_interest
             from {{ source('app', 'profiles') }}
                      left join p_int on p_int.profile_id = profiles.id
                      left join {{ ref('tickers') }} on true
                      left join t_int_sim_dif using (interest_id, symbol)
             group by profile_id, symbol
         ),
     interest_matches as
         (
             select profile_id,
                    symbol,
                    json_agg(interest_id) as interest_matches
             from (
                      select profile_id,
                             symbol,
                             interest_id,
                             sim_dif,
                             row_number()
                             over (partition by profile_id, symbol order by sim_dif desc nulls last) as row_number
                      from {{ source('app', 'profiles') }}
                               left join p_int on p_int.profile_id = profiles.id
                               left join {{ ref('tickers') }} on true
                               left join t_int_sim_dif using (interest_id, symbol)
                      order by profile_id, symbol, sim_dif desc nulls last
                  ) t
             where row_number <= 2
               and sim_dif is not null
               and sim_dif > 0
             group by profile_id, symbol
         )
select profile_id,
       symbol,
       (((match_comp_risk + match_comp_category + match_comp_interest) / 3 / 2 + 0.5) * 100)::int as match_score,
       (match_comp_risk / 2 + 0.5 > 0.3)::int + (match_comp_risk / 2 + 0.5 > 0.7)::int            as fits_risk,
       match_comp_risk / 2 + 0.5                                                                  as risk_similarity,
       (match_comp_category / 2 + 0.5 > 0.3)::int + (match_comp_category / 2 + 0.5 > 0.7)::int    as fits_categories,
       (match_comp_interest / 2 + 0.5 > 0.3)::int + (match_comp_interest / 2 + 0.5 > 0.7)::int    as fits_interests,
       coalesce(category_matches::text, '[]')                                                     as category_matches,
       coalesce(interest_matches::text, '[]')                                                     as interest_matches,
       now()                                                                                      as updated_at,
       category_similarity.match_comp_category / 2 + 0.5                                          as category_similarity,
       interest_similarity.match_comp_interest / 2 + 0.5                                          as interest_similarity
from risk_similarity
         left join category_similarity using (profile_id, symbol)
         left join category_matches using (profile_id, symbol)
         left join interest_similarity using (profile_id, symbol)
         left join interest_matches using (profile_id, symbol)
where profile_id = 5
order by symbol