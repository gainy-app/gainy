alter table "app"."drivewealth_accounts"
    add column "is_artificial" bool not null default false;
alter table "app"."drivewealth_portfolios"
    add column "is_artificial" bool not null default false;
