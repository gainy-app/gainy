{{
  config(
    materialized = "view",
  )
}}

select profiles.id                                               as profile_id,
       profile_scoring_settings.profile_id is not null           as is_personalization_enabled,
       coalesce(profile_flags.is_region_changing_allowed, false) as is_region_changing_allowed,
       coalesce(profile_flags.is_trading_enabled, false)         as is_trading_enabled,
       coalesce(not_viewed_notifications_count, 0)::int          as not_viewed_notifications_count
from {{ source('app', 'profiles') }}
         left join {{ source('app', 'profile_flags') }} on profile_flags.profile_id = profiles.id
         left join {{ source('app', 'profile_scoring_settings') }} on profile_scoring_settings.profile_id = profiles.id
         left join (
                       select profile_id, count(notification_uuid) as not_viewed_notifications_count
                       from {{ ref('notifications') }}
                       where not is_viewed
                       group by profile_id
                   ) notification_stats on notification_stats.profile_id = profiles.id
