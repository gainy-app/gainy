create or replace view app.analytics_profiles as
select id, gender, created_at, user_id
from app.profiles;
