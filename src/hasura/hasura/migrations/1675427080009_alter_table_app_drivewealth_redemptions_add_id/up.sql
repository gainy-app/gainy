alter table app.drivewealth_portfolios
    add column "pending_redemptions_amount_sum" numeric not null default 0;

with pending_redemptions as
         (
             select trading_account_ref_id, sum((data ->> 'amount')::numeric) as amount
             from app.drivewealth_redemptions
             where status in ('Approved', 'RIA_Approved')
             group by trading_account_ref_id
         )
update app.drivewealth_portfolios
set pending_redemptions_amount_sum = amount
from pending_redemptions
where pending_redemptions.trading_account_ref_id = drivewealth_portfolios.drivewealth_account_id;
