{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "interest_id__industry_id" ON {{ this }} (interest_id, industry_id)',
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

/* THE MODEL FILE IS AUTO GENERATED DURING BUILD, YOU SHOULD NOT EDIT THE MODEL, EDIT THE TEMPLATE INSTEAD  */

with
	ct as (
		select name as g_industry, id as g_industry_id from {{ ref('gainy_industries') }}
	),
    tmp_interest_industries as
         (
-- __SELECT__ --
         )
SELECT distinct (tii.interest_id, '_', tii.industry_id)::varchar as id,
        tii.interest_id,
        tii.industry_id,
        now() as updated_at
from tmp_interest_industries tii
         join {{ ref('gainy_industries') }} gi on tii.industry_id = gi.id
         join {{ ref ('interests') }} i on tii.interest_id = i.id

