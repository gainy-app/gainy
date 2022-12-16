alter table "app"."drivewealth_deposits" drop constraint "drivewealth_deposits_money_flow_id_fkey",
  add constraint "drivewealth_deposits_money_flow_id_fkey"
  foreign key ("money_flow_id")
  references "app"."trading_money_flow"
  ("id") on update restrict on delete restrict;
