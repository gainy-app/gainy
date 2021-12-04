alter table "app"."profile_holdings"
    drop constraint "profile_holdings_plaid_access_token_id_fkey",
    drop plaid_access_token_id;

alter table "app"."profile_portfolio_accounts"
    drop constraint "profile_portfolio_accounts_plaid_access_token_id_fkey",
    drop plaid_access_token_id;

alter table "app"."profile_portfolio_transactions"
    drop constraint "profile_portfolio_transactions_plaid_access_token_id_fkey",
    drop plaid_access_token_id;

