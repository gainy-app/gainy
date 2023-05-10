drop table "app"."corporate_action_drivewealth_transaction_link";
drop table "app"."corporate_action_adjustments";

alter table "app"."trading_orders"
    drop note;

alter table "app"."trading_collection_versions"
    drop note;
