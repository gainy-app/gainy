alter table app.trading_accounts
    add column account_no varchar;

update app.trading_accounts
    set account_no = drivewealth_accounts.ref_no
from app.drivewealth_accounts
where drivewealth_accounts.trading_account_id = trading_accounts.id
  and account_no is null;
