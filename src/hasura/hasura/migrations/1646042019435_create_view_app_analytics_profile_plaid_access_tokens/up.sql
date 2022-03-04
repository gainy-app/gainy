create view app.analytics_profile_plaid_access_tokens as
select id, profile_id, created_at, institution_id
from app.profile_plaid_access_tokens;
