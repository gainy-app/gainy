alter table "app"."trading_money_flow"
    drop column error_message;

alter table app.drivewealth_deposits
    drop constraint drivewealth_deposits_trading_account_ref_id_fkey;
alter table app.drivewealth_deposits
    add foreign key (trading_account_ref_id) references app.drivewealth_accounts
        on update cascade on delete cascade;

alter table app.drivewealth_deposits
    drop constraint drivewealth_deposits_bank_account_ref_id_fkey;
alter table app.drivewealth_deposits
    add foreign key (bank_account_ref_id) references app.drivewealth_bank_accounts
        on update cascade on delete cascade;

alter table app.drivewealth_deposits
    drop constraint drivewealth_deposits_money_flow_id_fkey;
alter table app.drivewealth_deposits
    add foreign key (money_flow_id) references app.trading_money_flow
        on update set null on delete set null;

alter table app.drivewealth_redemptions
    drop constraint drivewealth_redemptions_trading_account_ref_id_fkey;
alter table app.drivewealth_redemptions
    add foreign key (trading_account_ref_id) references app.drivewealth_accounts
        on update cascade on delete cascade;

alter table app.drivewealth_redemptions
    drop constraint drivewealth_redemptions_bank_account_ref_id_fkey;
alter table app.drivewealth_redemptions
    add foreign key (bank_account_ref_id) references app.drivewealth_bank_accounts
        on update cascade on delete cascade;

alter table app.drivewealth_redemptions
    drop constraint drivewealth_redemptions_money_flow_id_fkey;
alter table app.drivewealth_redemptions
    add foreign key (money_flow_id) references app.trading_money_flow
        on update restrict on delete restrict;
