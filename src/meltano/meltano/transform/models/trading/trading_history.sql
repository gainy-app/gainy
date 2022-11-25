{{
  config(
    materialized = "incremental",
    unique_key = "uniq_id",
    tags = ["realtime"],
    post_hook=[
      pk('uniq_id'),
      fk('profile_id', 'app', 'profiles', 'id'),
      fk('trading_collection_version_id', 'app', 'trading_collection_versions', 'id'),
      'create index if not exists "th_profile_id_datetime_type" ON {{ this }} (profile_id, datetime, type)',
    ]
  )
}}


with data as
         (
             select profile_id,
                    case when amount > 0 then 'deposit' else 'withdraw' end as type,
                    trading_money_flow.id,
                    null::int                                               as trading_collection_version_id,
                    case when amount > 0 then 'Deposit' else 'Withdraw' end as name,
                    created_at                                              as datetime,
                    amount,
                    json_build_object('deposit', amount > 0,
                                      'withdraw', amount < 0,
                                      'pending', status = 'PENDING' or status is null,
                                      'error', coalesce(status, '') = 'FAILED'
                        )                                                   as tags
             from {{ source('app', 'trading_money_flow') }}

             union all

             select profile_id,
                    'trading_fee' as type,
                    invoices.id,
                    null::int     as trading_collection_version_id,
                    'Service fee' as name,
                    created_at    as datetime,
                    amount,
                    json_build_object('fee', true,
                                      'pending', status = 'PENDING' or status is null,
                                      'error', coalesce(status, '') = 'FAILED'
                        )         as tags
             from {{ source('app', 'invoices') }}

             union all

             select profile_id,
                    'ttf_transaction'                               as type,
                    trading_collection_versions.id,
                    trading_collection_versions.id                  as trading_collection_version_id,
                    collections.name,
                    created_at                                      as datetime,
                    trading_collection_versions.target_amount_delta as amount,
                    json_build_object('ttf', true,
                                      'buy', target_amount_delta > 0,
                                      'sell', target_amount_delta < 0,
                                      'pending', status in ('PENDING', 'PENDING_EXECUTION') or status is null,
                                      'cancelled', coalesce(status, '') = 'CANCELLED',
                                      'error', coalesce(status, '') = 'FAILED'
                        )                                           as tags
             from {{ source('app', 'trading_collection_versions') }}
                      join {{ ref('collections') }} on collections.id = trading_collection_versions.collection_id
         )
select data.profile_id,
       data.trading_collection_version_id,
       data.type,
       data.name,
       data.datetime,
       data.amount::double precision,
       data.tags,
       data.profile_id || '_' || data.type || '_' || data.id as uniq_id
from data

{% if is_incremental() %}
         left join {{ this }} old_data on old_data.uniq_id = data.profile_id || '_' || data.type || '_' || data.id
{% endif %}

where data.profile_id is not null

{% if is_incremental() %}
  and (old_data.uniq_id is null
   or data.datetime != old_data.datetime
   or data.amount != old_data.amount
   or data.tags::text != old_data.tags::text)
{% endif %}
