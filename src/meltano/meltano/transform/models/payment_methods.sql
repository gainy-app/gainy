{{
  config(
    materialized = "view",
  )
}}


select payment_methods.id,
       profile_id,
       provider,
       drivewealth_accounts.ref_no as account_no
from {{ source('app', 'payment_methods') }}
         left join {{ source('app', 'drivewealth_accounts') }} on drivewealth_accounts.payment_method_id = payment_methods.id
