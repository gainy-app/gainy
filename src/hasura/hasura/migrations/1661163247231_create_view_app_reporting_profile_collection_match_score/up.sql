create view app.reporting_profile_collection_match_score as
select *
from app.profile_collection_match_score
join app.profiles on profiles.id = profile_collection_match_score.profile_id
where email not ilike '%test%@gainy.app';
