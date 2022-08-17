insert into app.plaid_institutions (name, ref_id)
values ('Demo', 'demo')
on conflict do nothing;

INSERT INTO app.profile_plaid_access_tokens (profile_id, access_token, item_id, institution_id)
select profiles.id,
       'portfolio_demo_' || gen_random_uuid(),
       'portfolio_demo_' || profiles.id,
       plaid_institutions.id
from app.profiles
         left join app.plaid_institutions on plaid_institutions.ref_id = 'demo'
where profiles.id = 1
on conflict do nothing;

INSERT INTO app.profile_plaid_access_tokens (profile_id, access_token, item_id)
select profiles.id,
       'access-sandbox-1b188d63-fea5-433e-aee9-30ba19cebda6',
       'Rbe7Mk36yxsZamjZLMGrtnpv3W6zx7uRwBdga'
from app.profiles
where profiles.id = 2
on conflict do nothing;

INSERT INTO app.profile_portfolio_accounts (ref_id, balance_current, balance_iso_currency_code,
                                            mask, name, official_name, subtype, type, profile_id,
                                            plaid_access_token_id)
select 'demo_' || profile_id,
       0,
       'USD',
       '0000',
       'Portfolio Test',
       'Portfolio Test',
       'checking',
       'investment',
       profile_id,
       profile_plaid_access_tokens.id
from app.profile_plaid_access_tokens
where profile_id = 1
on conflict do nothing;

INSERT INTO app.portfolio_securities (iso_currency_code, name, ref_id,
                                      ticker_symbol, type)
select 'USD',
       t.name,
       'demo_' || t.ticker_symbol,
       t.ticker_symbol,
       t.type
from (
         select *
         from (
                  values ('AAPL 06/21/24 225.00 Call', 'AAPL240621C00225000', 'derivative'),
                         ('Bitcoin', 'BTC', 'cryptocurrency'),
                         ('Apple Inc.', 'AAPL', 'equity'),
                         ('SPDR S&P 500', 'SPY', 'etf'),
                         ('PLTR 01/19/24 13.00 Put', 'PLTR240119P00013000', 'derivative')
              ) t ("name", "ticker_symbol", "type")
     ) t
         left join app.portfolio_securities as old_data using (ticker_symbol)
where old_data is null
on conflict do nothing;

INSERT INTO app.profile_holdings (iso_currency_code, quantity, security_id, profile_id, account_id, ref_id,
                                  plaid_access_token_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) 'USD',
      quantity,
      portfolio_securities.id,
      profile_id,
      profile_portfolio_accounts.id,
      'portfolio_demo_' || profile_id || '_' || ticker_symbol,
      profile_portfolio_accounts.plaid_access_token_id
from (
         values (1, 0.1, 'BTC'),
                (1, 2, 'AAPL'),
                (1, 3, 'SPY'),
                (1, 1, 'PLTR240119P00013000')
     ) t ("profile_id", "quantity", "ticker_symbol")
         join app.profile_portfolio_accounts using (profile_id)
         join app.portfolio_securities using (ticker_symbol)
on conflict do nothing;

insert into app.profile_portfolio_transactions (plaid_access_token_id, amount, date, fees, iso_currency_code, name,
                                                price, quantity, subtype, type, ref_id, security_id, profile_id,
                                                account_id)
select distinct on (
    profile_id,
    ticker_symbol
    ) profile_portfolio_accounts.plaid_access_token_id,
      t.amount,
      t.date::date,
      0,
      'USD',
      t.name,
      t.price,
      t.quantity,
      t.subtype,
      t.type,
      'portfolio_demo_' || profile_id || '_' || ticker_symbol,
      portfolio_securities.id,
      t.profile_id,
      profile_portfolio_accounts.id
from (
         values (1, 341.2, '2018-12-08', 'BUY Bitcoin', 3412, 0.1, 'buy', 'buy', 'BTC'),
                (1, 753.69, '2017-09-01', 'BUY SPY', 251.23, 3, 'buy', 'buy', 'SPY'),
                (1, 297.94, '2021-09-10', 'BUY Apple Inc.', 148.97, 2, 'buy', 'buy', 'AAPL'),
                (1, 2.65, '2021-11-29', 'BUY PLTR 01/19/24 13.00 Put', 2.65, 1, 'buy', 'buy', 'PLTR240119P00013000')
     ) t ("profile_id", "amount", "date", "name", "price", "quantity", "subtype", "type", "ticker_symbol")
         join app.profile_portfolio_accounts using (profile_id)
         join app.portfolio_securities using (ticker_symbol)
on conflict do nothing;
