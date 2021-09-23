with profile_category_vectors as (
    select profile_id, json_object_agg(category_id, 1.0) as profile_category_vector
    from app.profile_categories
    group by profile_id
)
select p.id, pcv.profile_category_vector
from app.profiles p
left join profile_category_vectors pcv
on p.id = pcv.profile_id
where p.id = '{0}';