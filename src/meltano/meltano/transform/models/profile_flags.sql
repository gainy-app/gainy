{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

select profiles.id                                               as profile_id,
       profile_scoring_settings.profile_id is not null           as is_personalization_enabled,
       coalesce(profile_flags.is_region_changing_allowed, false) as is_region_changing_allowed,
       coalesce(profile_flags.is_trading_enabled, false)         as is_trading_enabled
from {{ source('app', 'profiles') }}
         left join {{ source('app', 'profile_flags') }} on profile_flags.profile_id = profiles.id
         left join {{ source('app', 'profile_scoring_settings') }} on profile_scoring_settings.profile_id = profiles.id
