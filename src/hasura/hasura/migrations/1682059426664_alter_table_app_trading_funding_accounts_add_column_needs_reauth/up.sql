alter table "app"."trading_funding_accounts"
    add column "needs_reauth" boolean not null default 'false';

update app.trading_funding_accounts
set needs_reauth = true
from app.profile_plaid_access_tokens
where profile_plaid_access_tokens.id = trading_funding_accounts.plaid_access_token_id
  and profile_plaid_access_tokens.needs_reauth_since is not null;
