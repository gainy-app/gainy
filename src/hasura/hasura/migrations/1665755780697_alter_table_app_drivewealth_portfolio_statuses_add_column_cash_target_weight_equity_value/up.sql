alter table "app"."drivewealth_portfolio_statuses"
    add column "cash_target_weight"          numeric,
    add column "last_portfolio_rebalance_at" timestamptz,
    add column "next_portfolio_rebalance_at" timestamptz,
    add column "equity_value"                numeric;

update "app"."drivewealth_portfolio_statuses"
set equity_value = cash_value / cash_actual_weight
where equity_value is null;
