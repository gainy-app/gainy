drop view app.reporting_profile_ticker_match_score;
create view app.reporting_profile_ticker_match_score as
select *
from app.profile_ticker_match_score
         join app.profiles on profiles.id = profile_ticker_match_score.profile_id
where email not ilike '%test%@gainy.app';
