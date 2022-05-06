INSERT INTO app.profiles (email, first_name, last_name, gender, user_id)
select 'portfolio-test-' || idx || '@gainy.app',
       'fn',
       'ln',
       0,
       'user_id_portfolio_test_' || idx
from generate_series(1, 26) idx
on conflict do nothing;

INSERT INTO app.profile_plaid_access_tokens (profile_id, access_token, item_id)
select profiles.id,
       'portfolio_test_' || gen_random_uuid(),
       'item_id_portfolio_test_' || profiles.id
from app.profiles
         left join app.profile_plaid_access_tokens on profiles.id = profile_plaid_access_tokens.profile_id
where email like 'portfolio-test-%@gainy.app'
  and profile_plaid_access_tokens.id is null
on conflict do nothing;

INSERT INTO app.profile_portfolio_accounts (ref_id, balance_current, balance_iso_currency_code,
                                            mask, name, official_name, subtype, type, profile_id,
                                            plaid_access_token_id)
select 'portfolio_test_' || account_type || '_' || profile_id,
       100,
       'USD',
       '0000',
       'Portfolio Test',
       'Portfolio Test',
       'checking',
       account_type,
       profile_id,
       profile_plaid_access_tokens.id
from app.profile_plaid_access_tokens
         join app.profiles on profiles.id = profile_plaid_access_tokens.profile_id
         join (
                  select *
                  from (
                           values ('primary'), ('secondary')
                       ) t ("account_type")
              ) t on true
where email like 'portfolio-test-%@gainy.app'
on conflict do nothing;

INSERT INTO app.profile_holdings (iso_currency_code, quantity, security_id, profile_id, account_id, ref_id,
                                  plaid_access_token_id)
select 'USD',
       quantity,
       security_id,
       profile_id,
       profile_portfolio_accounts.id,
       'portfolio_test_' || profile_id || '_' || ticker_symbol,
       profile_portfolio_accounts.plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 2 else 100 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
where email in (
                   select 'portfolio-test-' || idx || '@gainy.app' from generate_series(1, 13) idx
               )
  and profile_portfolio_accounts.type = 'primary'
on conflict do nothing;

-- profile 1 with holdings without transactions at all
-- profile 14 without holdings without transactions at all

-- profile 2 with holdings with one buy transaction on the primary account
-- profile 15 without holdings with one buy transaction on the primary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) quantity * adjusted_close,
      date,
      1.5,
      'BUY Apple Inc.',
      adjusted_close,
      quantity,
      'buy',
      'buy',
      'portfolio-test-' || profile_id || '-' || ticker_symbol || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 2 else 100 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '4 days'
where email in ('portfolio-test-2@gainy.app', 'portfolio-test-15@gainy.app')
  and profile_portfolio_accounts.type = 'primary'
order by profile_id, ticker_symbol, historical_prices.date desc
on conflict do nothing;

-- profile 3 with holdings with one sell transaction on the primary account
-- profile 16 without holdings with one sell transaction on the primary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) quantity * adjusted_close,
      date,
      1.5,
      'SELL Apple Inc.',
      adjusted_close,
      quantity,
      'sell',
      'sell',
      'portfolio-test-' || profile_id || '-' || ticker_symbol || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 1 else 10 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '4 days'
where email in ('portfolio-test-3@gainy.app', 'portfolio-test-16@gainy.app')
  and profile_portfolio_accounts.type = 'primary'
order by profile_id, ticker_symbol, historical_prices.date desc
on conflict do nothing;

-- profile 4 with holdings with one buy transaction on the secondary account
-- profile 17 without holdings with one buy transaction on the secondary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) quantity * adjusted_close,
      date,
      1.5,
      'BUY Apple Inc.',
      adjusted_close,
      quantity,
      'buy',
      'buy',
      'portfolio-test-' || profile_id || '-' || ticker_symbol || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 2 else 100 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '4 days'
where email in ('portfolio-test-4@gainy.app', 'portfolio-test-17@gainy.app')
  and profile_portfolio_accounts.type = 'secondary'
order by profile_id, ticker_symbol, historical_prices.date desc
on conflict do nothing;

-- profile 5 with holdings with one sell transaction on the secondary account
-- profile 18 without holdings with one sell transaction on the secondary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) quantity * adjusted_close,
      date,
      1.5,
      'SELL Apple Inc.',
      adjusted_close,
      quantity,
      'sell',
      'sell',
      'portfolio-test-' || profile_id || '-' || ticker_symbol || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 1 else 10 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '4 days'
where email in ('portfolio-test-5@gainy.app', 'portfolio-test-18@gainy.app')
  and profile_portfolio_accounts.type = 'secondary'
order by profile_id, ticker_symbol, historical_prices.date desc
on conflict do nothing;

-- profile 6 with holdings with buy-sell transactions on the primary account
-- profile 19 without holdings with buy-sell transactions on the primary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) quantity * adjusted_close,
      date,
      1.5,
      'BUY Apple Inc.',
      adjusted_close,
      quantity,
      'buy',
      'buy',
      'portfolio-test-' || profile_id || '-' || ticker_symbol || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 2 else 100 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '4 days'
where email in ('portfolio-test-6@gainy.app', 'portfolio-test-19@gainy.app')
  and profile_portfolio_accounts.type = 'primary'
order by profile_id, ticker_symbol, historical_prices.date desc
on conflict do nothing;

INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_portfolio_accounts.profile_id,
    ticker_symbol
    ) t.quantity * historical_prices.adjusted_close,
      historical_prices.date,
      1.5,
      'SELL Apple Inc.',
      historical_prices.adjusted_close,
      t.quantity,
      'sell',
      'sell',
      'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-1',
      t.security_id,
      profile_portfolio_accounts.profile_id,
      profile_portfolio_accounts.id,
      profile_portfolio_accounts.plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 1 else 10 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
    -- buy transaction
         join app.profile_portfolio_transactions buy_tx
              on buy_tx.ref_id =
                 'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-0'
         join historical_prices
              on code = ticker_symbol and historical_prices.date > buy_tx.date
where email in ('portfolio-test-6@gainy.app', 'portfolio-test-19@gainy.app')
  and profile_portfolio_accounts.type = 'primary'
order by profile_id, ticker_symbol, historical_prices.date
on conflict do nothing;

-- profile 7 with holdings with buy-sell transactions on the primary-secondary account
-- profile 20 without holdings with buy-sell transactions on the primary-secondary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) quantity * adjusted_close,
      date,
      1.5,
      'BUY Apple Inc.',
      adjusted_close,
      quantity,
      'buy',
      'buy',
      'portfolio-test-' || profile_id || '-' || ticker_symbol || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 2 else 100 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '4 days'
where email in ('portfolio-test-7@gainy.app', 'portfolio-test-20@gainy.app')
  and profile_portfolio_accounts.type = 'primary'
order by profile_id, ticker_symbol, historical_prices.date desc
on conflict do nothing;

INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_portfolio_accounts.profile_id,
    ticker_symbol
    ) t.quantity * historical_prices.adjusted_close,
      historical_prices.date,
      1.5,
      'SELL Apple Inc.',
      historical_prices.adjusted_close,
      t.quantity,
      'sell',
      'sell',
      'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-1',
      t.security_id,
      profile_portfolio_accounts.profile_id,
      profile_portfolio_accounts.id,
      profile_portfolio_accounts.plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 1 else 10 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
    -- buy transaction
         join app.profile_portfolio_transactions buy_tx
              on buy_tx.ref_id =
                 'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-0'
         join historical_prices
              on code = ticker_symbol and historical_prices.date > buy_tx.date
where email in ('portfolio-test-7@gainy.app', 'portfolio-test-20@gainy.app')
  and profile_portfolio_accounts.type = 'secondary'
order by profile_id, ticker_symbol, historical_prices.date
on conflict do nothing;

-- profile 8 with holdings with buy-sell transactions on the secondary-primary account
-- profile 21 without holdings with buy-sell transactions on the secondary-primary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) quantity * adjusted_close,
      date,
      1.5,
      'BUY Apple Inc.',
      adjusted_close,
      quantity,
      'buy',
      'buy',
      'portfolio-test-' || profile_id || '-' || ticker_symbol || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 2 else 100 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '4 days'
where email in ('portfolio-test-8@gainy.app', 'portfolio-test-21@gainy.app')
  and profile_portfolio_accounts.type = 'secondary'
order by profile_id, ticker_symbol, historical_prices.date desc
on conflict do nothing;

INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_portfolio_accounts.profile_id,
    ticker_symbol
    ) t.quantity * historical_prices.adjusted_close,
      historical_prices.date,
      1.5,
      'SELL Apple Inc.',
      historical_prices.adjusted_close,
      t.quantity,
      'sell',
      'sell',
      'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-1',
      t.security_id,
      profile_portfolio_accounts.profile_id,
      profile_portfolio_accounts.id,
      profile_portfolio_accounts.plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 1 else 10 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
    -- buy transaction
         join app.profile_portfolio_transactions buy_tx
              on buy_tx.ref_id =
                 'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-0'
         join historical_prices
              on code = ticker_symbol and historical_prices.date > buy_tx.date
where email in ('portfolio-test-8@gainy.app', 'portfolio-test-21@gainy.app')
  and profile_portfolio_accounts.type = 'primary'
order by profile_id, ticker_symbol, historical_prices.date
on conflict do nothing;

-- profile 9 with holdings with buy-sell transactions on the secondary account
-- profile 22 without holdings with buy-sell transactions on the secondary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) quantity * adjusted_close,
      date,
      1.5,
      'BUY Apple Inc.',
      adjusted_close,
      quantity,
      'buy',
      'buy',
      'portfolio-test-' || profile_id || '-' || ticker_symbol || '-' || index,
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select 50 as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '4 days'
         join (
                  select *
                  from (
                           values (0, 1)
                       ) t ("index")
              ) t2
              on true
where email in ('portfolio-test-9@gainy.app', 'portfolio-test-22@gainy.app')
  and profile_portfolio_accounts.type = 'secondary'
order by profile_id, ticker_symbol, historical_prices.date desc
on conflict do nothing;

INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_portfolio_accounts.profile_id,
    ticker_symbol
    ) t.quantity * historical_prices.adjusted_close,
      historical_prices.date,
      1.5,
      'SELL Apple Inc.',
      historical_prices.adjusted_close,
      t.quantity,
      'sell',
      'sell',
      'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-2',
      t.security_id,
      profile_portfolio_accounts.profile_id,
      profile_portfolio_accounts.id,
      profile_portfolio_accounts.plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 1 else 10 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
    -- buy transaction
         join app.profile_portfolio_transactions buy_tx
              on buy_tx.ref_id =
                 'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-0'
         join historical_prices
              on code = ticker_symbol and historical_prices.date > buy_tx.date
where email in ('portfolio-test-9@gainy.app', 'portfolio-test-22@gainy.app')
  and profile_portfolio_accounts.type = 'secondary'
order by profile_id, ticker_symbol, historical_prices.date
on conflict do nothing;

-- profile 10 with holdings with sell-buy transactions on the primary account
-- profile 23 without holdings with sell-buy transactions on the primary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) quantity * adjusted_close,
      date,
      1.5,
      'SELL Apple Inc.',
      adjusted_close,
      quantity,
      'sell',
      'sell',
      'portfolio-test-' || profile_id || '-' || ticker_symbol || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 1 else 10 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '4 days'
where email in ('portfolio-test-10@gainy.app', 'portfolio-test-23@gainy.app')
  and profile_portfolio_accounts.type = 'primary'
order by profile_id, ticker_symbol, historical_prices.date desc
on conflict do nothing;

INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_portfolio_accounts.profile_id,
    ticker_symbol
    ) t.quantity * historical_prices.adjusted_close,
      historical_prices.date,
      1.5,
      'BUY Apple Inc.',
      historical_prices.adjusted_close,
      t.quantity,
      'buy',
      'buy',
      'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-1',
      t.security_id,
      profile_portfolio_accounts.profile_id,
      profile_portfolio_accounts.id,
      profile_portfolio_accounts.plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 2 else 100 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
    -- sell transaction
         join app.profile_portfolio_transactions sell_tx
              on sell_tx.ref_id =
                 'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-0'
         join historical_prices
              on code = ticker_symbol and historical_prices.date > sell_tx.date
where email in ('portfolio-test-10@gainy.app', 'portfolio-test-23@gainy.app')
  and profile_portfolio_accounts.type = 'primary'
order by profile_id, ticker_symbol, historical_prices.date
on conflict do nothing;

-- profile 11 with holdings with sell-buy transactions on the primary-secondary account
-- profile 24 without holdings with sell-buy transactions on the primary-secondary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) quantity * adjusted_close,
      date,
      1.5,
      'SELL Apple Inc.',
      adjusted_close,
      quantity,
      'sell',
      'sell',
      'portfolio-test-' || profile_id || '-' || ticker_symbol || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 1 else 10 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '4 days'
where email in ('portfolio-test-11@gainy.app', 'portfolio-test-24@gainy.app')
  and profile_portfolio_accounts.type = 'primary'
order by profile_id, ticker_symbol, historical_prices.date desc
on conflict do nothing;

INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_portfolio_accounts.profile_id,
    ticker_symbol
    ) t.quantity * historical_prices.adjusted_close,
      historical_prices.date,
      1.5,
      'BUY Apple Inc.',
      historical_prices.adjusted_close,
      t.quantity,
      'buy',
      'buy',
      'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-1',
      t.security_id,
      profile_portfolio_accounts.profile_id,
      profile_portfolio_accounts.id,
      profile_portfolio_accounts.plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 2 else 100 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
    -- sell transaction
         join app.profile_portfolio_transactions sell_tx
              on sell_tx.ref_id =
                 'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-0'
         join historical_prices
              on code = ticker_symbol and historical_prices.date > sell_tx.date
where email in ('portfolio-test-11@gainy.app', 'portfolio-test-24@gainy.app')
  and profile_portfolio_accounts.type = 'secondary'
order by profile_id, ticker_symbol, historical_prices.date
on conflict do nothing;

-- profile 12 with holdings with sell-buy transactions on the secondary-primary account
-- profile 25 without holdings with sell-buy transactions on the secondary-primary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) quantity * adjusted_close,
      date,
      1.5,
      'SELL Apple Inc.',
      adjusted_close,
      quantity,
      'sell',
      'sell',
      'portfolio-test-' || profile_id || '-' || ticker_symbol || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 1 else 10 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '4 days'
where email in ('portfolio-test-12@gainy.app', 'portfolio-test-25@gainy.app')
  and profile_portfolio_accounts.type = 'secondary'
order by profile_id, ticker_symbol, historical_prices.date desc
on conflict do nothing;

INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_portfolio_accounts.profile_id,
    ticker_symbol
    ) t.quantity * historical_prices.adjusted_close,
      historical_prices.date,
      1.5,
      'BUY Apple Inc.',
      historical_prices.adjusted_close,
      t.quantity,
      'buy',
      'buy',
      'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-1',
      t.security_id,
      profile_portfolio_accounts.profile_id,
      profile_portfolio_accounts.id,
      profile_portfolio_accounts.plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 2 else 100 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
    -- sell transaction
         join app.profile_portfolio_transactions sell_tx
              on sell_tx.ref_id =
                 'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-0'
         join historical_prices
              on code = ticker_symbol and historical_prices.date > sell_tx.date
where email in ('portfolio-test-12@gainy.app', 'portfolio-test-25@gainy.app')
  and profile_portfolio_accounts.type = 'primary'
order by profile_id, ticker_symbol, historical_prices.date
on conflict do nothing;

-- profile 13 with holdings with sell-buy transactions on the secondary account
-- profile 26 without holdings with sell-buy transactions on the secondary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) quantity * adjusted_close,
      date,
      1.5,
      'SELL Apple Inc.',
      adjusted_close,
      quantity,
      'sell',
      'sell',
      'portfolio-test-' || profile_id || '-' || ticker_symbol || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 1 else 10 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '4 days'
where email in ('portfolio-test-13@gainy.app', 'portfolio-test-26@gainy.app')
  and profile_portfolio_accounts.type = 'secondary'
order by profile_id, ticker_symbol, historical_prices.date desc
on conflict do nothing;

INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_portfolio_accounts.profile_id,
    ticker_symbol
    ) t.quantity * historical_prices.adjusted_close,
      historical_prices.date,
      1.5,
      'BUY Apple Inc.',
      historical_prices.adjusted_close,
      t.quantity,
      'buy',
      'buy',
      'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-1',
      t.security_id,
      profile_portfolio_accounts.profile_id,
      profile_portfolio_accounts.id,
      profile_portfolio_accounts.plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (
                  select case when type = 'derivative' then 2 else 100 end as quantity, id as security_id, ticker_symbol
                  from app.portfolio_securities
                  where ticker_symbol in ('AAPL', 'AAPL240621C00225000')
              ) t on true
    -- sell transaction
         join app.profile_portfolio_transactions sell_tx
              on sell_tx.ref_id =
                 'portfolio-test-' || profile_portfolio_accounts.profile_id || '-' || ticker_symbol || '-0'
         join historical_prices
              on code = ticker_symbol and historical_prices.date > sell_tx.date
where email in ('portfolio-test-13@gainy.app', 'portfolio-test-26@gainy.app')
  and profile_portfolio_accounts.type = 'secondary'
order by profile_id, ticker_symbol, historical_prices.date
on conflict do nothing;
