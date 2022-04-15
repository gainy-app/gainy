INSERT INTO app.profiles (email, first_name, last_name, gender, user_id)
select 'portfolio-test-' || idx || '@gainy.app',
       'fn',
       'ln',
       0,
       'user_id_portfolio_test_' || idx
from generate_series(1, 26) idx
on conflict do nothing;

-- INSERT INTO app.profile_scoring_settings (profile_id, created_at, risk_level, average_market_return, investment_horizon,
--                                           unexpected_purchases_source, damage_of_failure, stock_market_risk_level,
--                                           trading_experience, if_market_drops_20_i_will_buy,
--                                           if_market_drops_40_i_will_buy)
--
-- select 1, '2021-10-18 11:46:18.851570 +00:00', 0.5, 6, 0.5, 'checking_savings', 0.5, 'very_risky', 'never_tried', 0.5,
--        0.5
-- from app.profiles
-- where email like 'portfolio-test-%@gainy.app';

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
         join (select * from (values ('primary'), ('secondary')) t ("account_type")) t on true
where email like 'portfolio-test-%@gainy.app'
on conflict do nothing;

INSERT INTO app.profile_holdings (iso_currency_code, quantity, security_id, profile_id, account_id, ref_id,
                                  plaid_access_token_id)
select 'USD',
       quantity,
       security_id,
       profile_id,
       profile_portfolio_accounts.id,
       'portfolio_test_' || gen_random_uuid(),
       profile_portfolio_accounts.plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles on profiles.id = profile_portfolio_accounts.profile_id
         join (select 100 as quantity, id as security_id from app.portfolio_securities where ticker_symbol = 'AAPL') t
              on true
where email in (select 'portfolio-test-' || idx || '@gainy.app' from generate_series(1, 13) idx)
  and profile_portfolio_accounts.type = 'primary'
on conflict do nothing;

-- profile 1 without holdings without transactions at all

-- profile 2 without holdings with one buy transaction on the primary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id
    ) quantity * adjusted_close,
      date,
      1.5,
      'BUY Apple Inc.',
      adjusted_close,
      quantity,
      'buy',
      'buy',
      'portfolio-test-' || profile_id || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (select 100 as quantity, id as security_id, ticker_symbol
               from app.portfolio_securities
               where ticker_symbol = 'AAPL') t
              on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '3 days'
where email = 'portfolio-test-2@gainy.app'
  and profile_portfolio_accounts.type = 'primary'
order by profile_id, historical_prices.date desc
on conflict do nothing;

-- profile 3 without holdings with one sell transaction on the primary account
INSERT INTO app.profile_portfolio_transactions (amount, date, fees, name, price, quantity,
                                                subtype, type, ref_id, security_id, profile_id, account_id,
                                                plaid_access_token_id)
select distinct on (
    profile_id
    ) quantity * adjusted_close,
      date,
      1.5,
      'SELL Apple Inc.',
      adjusted_close,
      quantity,
      'sell',
      'sell',
      'portfolio-test-' || profile_id || '-0',
      security_id,
      profile_id,
      profile_portfolio_accounts.id,
      plaid_access_token_id
from app.profile_portfolio_accounts
         join app.profiles
              on profiles.id = profile_portfolio_accounts.profile_id
         join (select 10 as quantity, id as security_id, ticker_symbol
               from app.portfolio_securities
               where ticker_symbol = 'AAPL') t
              on true
         join historical_prices
              on code = ticker_symbol and date between now() - interval '7 days' and now() - interval '3 days'
where email = 'portfolio-test-3@gainy.app'
  and profile_portfolio_accounts.type = 'primary'
order by profile_id, historical_prices.date desc
on conflict do nothing;

-- profile 4 without holdings with one buy transaction on the secondary account
-- profile 5 without holdings with one sell transaction on the secondary account
-- profile 6 without holdings with buy-sell transactions on the primary account
-- profile 7 without holdings with buy-sell transactions on the primary-secondary account
-- profile 8 without holdings with buy-sell transactions on the secondary-primary account
-- profile 9 without holdings with buy-sell transactions on the secondary account
-- profile 10 without holdings with sell-buy transactions on the primary account
-- profile 11 without holdings with sell-buy transactions on the primary-secondary account
-- profile 12 without holdings with sell-buy transactions on the secondary-primary account
-- profile 13 without holdings with sell-buy transactions on the secondary account

-- profile 14 without transactions at all
-- profile 15 with one buy transaction on the primary account
-- profile 16 with one sell transaction on the primary account
-- profile 17 with one buy transaction on the secondary account
-- profile 18 with one sell transaction on the secondary account
-- profile 19 with buy-sell transactions on the primary account
-- profile 20 with buy-sell transactions on the primary-secondary account
-- profile 21 with buy-sell transactions on the secondary-primary account
-- profile 22 with buy-sell transactions on the secondary account
-- profile 23 with sell-buy transactions on the primary account
-- profile 24 with sell-buy transactions on the primary-secondary account
-- profile 25 with sell-buy transactions on the secondary-primary account
-- profile 26 with sell-buy transactions on the secondary account