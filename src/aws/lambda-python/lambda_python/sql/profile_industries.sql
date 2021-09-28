with profile_industries as (
    select distinct ap.profile_id, tin.interest_id, tid.industry_id
    from app.profile_interests ap
             join ticker_interests tin
                  on ap.interest_id = tin.interest_id
             join ticker_industries tid
                  on tid.symbol = tin.symbol
),
     profile_industries_counts as (
         select profile_id, industry_id, count(*) as industry_count
         from profile_industries
         group by profile_id, industry_id
     ),
     profile_industry_vectors as (
         select profile_id, json_object_agg(industry_id, industry_count) as profile_industry_vector
         from profile_industries_counts
         group by profile_id
     )
select p.id, piv.profile_industry_vector
from app.profiles p
         left join profile_industry_vectors piv
                   on p.id = piv.profile_id
where p.id = '{0}';