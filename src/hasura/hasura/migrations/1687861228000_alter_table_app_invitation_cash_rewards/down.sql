drop table app.invitation_cash_rewards cascade;

alter table "app"."trading_money_flow"
    drop column "type";
