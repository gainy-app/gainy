alter table app.drivewealth_portfolios
    add column "last_equity_value"   numeric,
    add column "last_transaction_id" int,
    add foreign key ("last_transaction_id") REFERENCES "app"."drivewealth_transactions" ("id") ON UPDATE restrict ON DELETE restrict;

with last_transaction as
         (
             select account_id, max(id) as max_id
             from app.drivewealth_transactions
             group by account_id
         ),
     last_portfolio_status as
         (
             select distinct on (
                 drivewealth_portfolio_id
                 ) drivewealth_portfolio_id,
                   equity_value
             from app.drivewealth_portfolio_statuses
             order by drivewealth_portfolio_id desc, date desc, created_at desc
     )
update app.drivewealth_portfolios
set last_transaction_id = last_transaction.max_id,
    last_equity_value   = last_portfolio_status.equity_value
from last_transaction,
     last_portfolio_status
where last_transaction.account_id = drivewealth_portfolios.drivewealth_account_id
  and last_portfolio_status.drivewealth_portfolio_id = drivewealth_portfolios.drivewealth_account_id;

