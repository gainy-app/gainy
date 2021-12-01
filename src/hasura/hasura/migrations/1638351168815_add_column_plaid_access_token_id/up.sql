start transaction;

truncate "app"."profile_holdings";
truncate "app"."profile_portfolio_accounts";
truncate "app"."profile_portfolio_transactions";

alter table "app"."profile_holdings"
    add column "plaid_access_token_id" integer
        null;
alter table "app"."profile_holdings"
    add constraint "profile_holdings_plaid_access_token_id_fkey"
        foreign key ("plaid_access_token_id")
            references "app"."profile_plaid_access_tokens"
                ("id") on update cascade on delete cascade;

alter table "app"."profile_portfolio_accounts"
    add column "plaid_access_token_id" integer
        null;
alter table "app"."profile_portfolio_accounts"
    add constraint "profile_portfolio_accounts_plaid_access_token_id_fkey"
        foreign key ("plaid_access_token_id")
            references "app"."profile_plaid_access_tokens"
                ("id") on update cascade on delete cascade;

alter table "app"."profile_portfolio_transactions"
    add column "plaid_access_token_id" integer
        null;
alter table "app"."profile_portfolio_transactions"
    add constraint "profile_portfolio_transactions_plaid_access_token_id_fkey"
        foreign key ("plaid_access_token_id")
            references "app"."profile_plaid_access_tokens"
                ("id") on update cascade on delete cascade;

commit;