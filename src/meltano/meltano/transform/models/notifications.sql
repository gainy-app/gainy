{{
  config(
    materialized = "view",
  )
}}

select t.*,
       profile_notification_viewed.profile_id is not null as is_viewed
from (
         with profiles as
                  (
                      SELECT profiles.id                      as profile_id,
                             email ilike '%gainy.app'
                                 or email ilike '%test%'
                                 or last_name ilike '%test%'
                                 or first_name ilike '%test%' as is_test
                      FROM {{ source('app', 'profiles') }}
                  )
         select profile_id, uuid as notification_uuid, title::json, text::json, data::json, created_at
         from {{ source('app', 'notifications') }}
                  join profiles using (profile_id)
         where is_shown_in_app
           and (not notifications.is_test or profiles.is_test)

         union all

         select profiles.profile_id, uuid as notification_uuid, title::json, text::json, data::json, created_at
         from {{ source('app', 'notifications') }}
                  join profiles on true
         where is_shown_in_app
           and notifications.profile_id is null
     ) t
         left join {{ source('app', 'profile_notification_viewed') }} using (profile_id, notification_uuid)
