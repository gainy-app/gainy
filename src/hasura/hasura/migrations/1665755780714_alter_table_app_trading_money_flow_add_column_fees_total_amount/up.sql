alter table app.drivewealth_deposits
    add column fees_total_amount decimal;

alter table app.drivewealth_redemptions
    add column fees_total_amount decimal;

alter table app.trading_money_flow
    add column fees_total_amount decimal;
