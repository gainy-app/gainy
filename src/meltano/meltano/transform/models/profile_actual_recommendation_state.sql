select profile_scoring_settings.profile_id,
       md5(coalesce(categories_state, '') || '_' || coalesce(interests_state, '') || '_' ||
           coalesce(round(risk_score::numeric, 4)::text, '')) as state_hash
from {{ source('app', 'profile_scoring_settings') }}
         left join (
                       select profile_id,
                              array_agg(category_id order by category_id)::text as categories_state
                       from {{ source('app', 'profile_categories') }}
                       group by profile_id
                   ) categories_state using(profile_id)
         left join (
                       select profile_id,
                              array_agg(interest_id order by interest_id)::text as interests_state
                       from {{ source('app', 'profile_interests') }}
                       group by profile_id
                   ) interests_state using(profile_id)
where categories_state is not null
   or interests_state is not null
