alter table app.drivewealth_deposits
    drop column fees_total_amount;

alter table app.drivewealth_redemptions
    drop column fees_total_amount;

alter table app.trading_money_flow
    drop column fees_total_amount;
