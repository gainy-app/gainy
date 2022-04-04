{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


with collection_categories as
         (
             select *
             from (
                      select *,
                             row_number() over (partition by profile_id, collection_id order by cnt desc) as row_num
                      from (
                               select profile_id,
                                      collection_id,
                                      category_match_id,
                                      count(*) as cnt
                               from (
                                        select profile_id,
                                               collection_id,
                                               json_array_elements_text(category_matches::json)::int as category_match_id
                                        from {{ ref('ticker_collections') }}
                                                 join {{ source('app', 'profile_ticker_match_score') }} using (symbol)
                                    ) t
                               group by profile_id, collection_id, category_match_id
                           ) t
                  ) t
             where row_num <= 3
         ),
     collection_interests as
         (
             select *
             from (
                      select *,
                             row_number() over (partition by profile_id, collection_id order by cnt desc) as row_num
                      from (
                               select profile_id,
                                      collection_id,
                                      interest_match_id,
                                      count(*) as cnt
                               from (
                                        select profile_id,
                                               collection_id,
                                               json_array_elements_text(interest_matches::json)::int as interest_match_id
                                        from {{ ref('ticker_collections') }}
                                                 join {{ source('app', 'profile_ticker_match_score') }} using (symbol)
                                    ) t
                               group by profile_id, collection_id, interest_match_id
                           ) t
                  ) t
             where row_num <= 3
         )
select t.*,
       uniq_id as collection_uniq_id
from (
         select profile_id,
                collection_id,
                category_match_id::int as category_id,
                null                   as interest_id
         from collection_categories
         union all
         select profile_id,
                collection_id,
                null                   as category_id,
                interest_match_id::int as interest_id
         from collection_interests
     ) t
         join {{ ref('profile_collections') }}
              on (profile_collections.profile_id is null or
                  profile_collections.profile_id = t.profile_id)
                  and profile_collections.id = collection_id
