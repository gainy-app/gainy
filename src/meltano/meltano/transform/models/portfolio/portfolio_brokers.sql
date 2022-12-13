{{
  config(
    materialized = "incremental",
    unique_key = "uniq_id",
    tags = ["realtime"],
    post_hook=[
      pk('uniq_id'),
      fk('plaid_institution_id', 'app', 'plaid_institutions', 'id'),
    ]
  )
}}


with data as
    (
         select 'plaid_' || id as uniq_id,
                id             as plaid_institution_id,
                name
         from {{ source('app', 'plaid_institutions')}}

         union all

         select 'gainy_broker' as uniq_id,
                null::int      as plaid_institution_id,
                'Gainy'
    )
select data.*
from data

{% if is_incremental() %}
         left join {{ this }} old_data using (uniq_id)
where old_data.uniq_id is null
   or data.name != old_data.name
{% endif %}
