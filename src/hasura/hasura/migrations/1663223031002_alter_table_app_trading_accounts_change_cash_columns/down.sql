alter table app.trading_accounts
    alter column cash_available_for_trade type integer,
    alter column cash_available_for_withdrawal type integer,
    alter column cash_balance type integer;

