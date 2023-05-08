with profiles as
         (
             select id as profile_id,
                    is_personalization_enabled
             from {{ source('app', 'profiles')}}
                      left join {{ ref('profile_flags') }} pf on profiles.id = pf.profile_id
         ),
     section_collections as
         (
             select section_id,
                    collection_id::int,
                    row_number() over wnd as position
             from {{ source('gainy', 'section_collections') }}
             where _sdc_extracted_at > (
                                           select max(_sdc_extracted_at) - interval '1 minute'
                                           from raw_data.section_collections
                                       )
             window wnd as (partition by section_id, collection_id order by position::int)
     ),
     match_score_section as
         (
             select profile_id,
                    collection_uniq_id,
                    'match_score'         as section_id,
                    row_number() over wnd as position
             from {{ source('app', 'profile_collection_match_score')}}
                      join profiles using (profile_id)
                      join {{ ref('collections') }} on profile_collection_match_score.collection_id = collections.id
             where collections.enabled = '1'
               and collections.personalized = '0'
               and is_personalization_enabled
             window wnd as (partition by profile_id order by match_score desc nulls last)
             order by profile_collection_match_score.match_score desc nulls last
     )

select profile_id,
       collection_uniq_id,
       section_id,
       position::int
from match_score_section
where position <= 18

union all

(
    select profiles.profile_id,
           uniq_id as collection_uniq_id,
           section_id,
           position::int
    from section_collections
             join profiles on true
             join {{ ref('profile_collections') }} on profile_collections.id = section_collections.collection_id
    where profile_collections.enabled = '1'
      and profile_collections.personalized = '0'
      and position <= 18
    order by position
)
