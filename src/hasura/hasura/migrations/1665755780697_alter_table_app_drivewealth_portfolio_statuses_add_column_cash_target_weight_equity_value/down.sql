alter table "app"."drivewealth_portfolio_statuses"
    drop column "cash_target_weight",
    drop column "last_portfolio_rebalance_at",
    drop column "next_portfolio_rebalance_at",
    drop column "equity_value";
