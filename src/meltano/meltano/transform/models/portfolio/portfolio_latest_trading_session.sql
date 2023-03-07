{{
  config(
    materialized = "view",
  )
}}


select profile_id, date, min(datetime) as open_at
from {{ ref('portfolio_chart_skeleton') }}
         join (
                  select profile_id, max(date) as date
                  from {{ ref('portfolio_chart_skeleton') }}
                  where period = '1d'
                  group by profile_id
              ) t using (profile_id, date)
where period = '1d'
group by profile_id, date
