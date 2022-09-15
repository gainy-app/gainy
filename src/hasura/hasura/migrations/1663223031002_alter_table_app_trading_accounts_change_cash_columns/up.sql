alter table app.trading_accounts
    alter column cash_available_for_trade type double precision,
    alter column cash_available_for_withdrawal type double precision,
    alter column cash_balance type double precision;

