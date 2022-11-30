alter table app.trading_money_flow
    drop constraint trading_money_flow_funding_account_id_fkey;

alter table app.trading_money_flow
    add foreign key (funding_account_id) references app.trading_funding_accounts
        on update set null on delete set null;
