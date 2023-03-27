alter table app.trading_funding_accounts
    add column mask varchar;

update app.trading_funding_accounts
set mask = drivewealth_bank_accounts.bank_account_number
from app.drivewealth_bank_accounts
where drivewealth_bank_accounts.funding_account_id = trading_funding_accounts.id;
