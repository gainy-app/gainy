DROP VIEW "app"."analytics_profile_plaid_access_tokens";
CREATE VIEW "app"."analytics_profile_plaid_access_tokens" AS
 SELECT profile_plaid_access_tokens.id,
    profile_plaid_access_tokens.profile_id,
    profile_plaid_access_tokens.created_at::timestamp,
    profile_plaid_access_tokens.institution_id
   FROM app.profile_plaid_access_tokens;
