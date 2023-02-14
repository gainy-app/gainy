{{
  config(
    materialized = "view",
  )
}}

with holding_group_collection_tags as
         (
             select distinct profile_id,
                             holding_group_id,
                             category_id,
                             interest_id,
                             priority
             from (
                      select profile_id, holding_group_id, collection_id
                      from {{ ref('profile_holdings_normalized') }}
                      where collection_id is not null
                      group by profile_id, holding_group_id, collection_id
                  ) t
                      join {{ ref('collection_match_score_explanation') }} using (profile_id, collection_id)
         ),
     ticker_tags_ranked as
         (
             select *,
                    -row_number() over (partition by profile_id, holding_group_id order by sim_dif desc) as priority
             from (

                      select distinct profile_id,
                                      holding_group_id,
                                      category_id,
                                      null::int    as interest_id,
                                      max(sim_dif) as sim_dif
                      from {{ ref('profile_holdings_normalized') }}
                               join {{ ref('ticker_categories') }} on ticker_categories.symbol = ticker_symbol
                      where collection_id is null
                      group by profile_id, holding_group_id, category_id

                      union all

                      select distinct profile_id,
                                      holding_group_id,
                                      null::int    as category_id,
                                      interest_id,
                                      max(sim_dif) as sim_dif
                      from {{ ref('profile_holdings_normalized') }}
                               join {{ ref('ticker_interests') }} on ticker_interests.symbol = ticker_symbol
                      where collection_id is null
                      group by profile_id, holding_group_id, interest_id
                  ) t
     )
select distinct on (
    profile_id,
    holding_group_id,
    category_id,
    interest_id
    ) holding_group_id,
      profile_id,
      null::int  as collection_id,
      null::text as collection_uniq_id,
      category_id,
      interest_id,
      priority
from (
         select profile_id,
                holding_group_id,
                category_id,
                interest_id,
                priority::int
         from holding_group_collection_tags

         union all

         select profile_id,
                holding_group_id,
                category_id,
                interest_id,
                priority::int
         from ticker_tags_ranked
     ) t
