insert into app.plaid_institutions (id, name, ref_id, created_at, updated_at)
values (1, 'E*TRADE Financial', 'ins_129473', '2022-01-17 13:01:58.770740 +00:00', '2022-01-17 13:02:30.966450 +00:00');
ALTER SEQUENCE app.plaid_institutions_id_seq RESTART WITH 2;

INSERT INTO app.profile_plaid_access_tokens (id, profile_id, access_token, item_id, institution_id)
             VALUES (1, (SELECT id FROM app.profiles where email = 'test@gainy.app'), 'foo', 'bar', 1),
                    (2, (SELECT id FROM app.profiles where email = 'test2@gainy.app'), 'access-sandbox-1b188d63-fea5-433e-aee9-30ba19cebda6', 'Rbe7Mk36yxsZamjZLMGrtnpv3W6zx7uRwBdga', 1);
ALTER SEQUENCE app.profile_plaid_access_tokens_id_seq RESTART WITH 3;

insert into app.profile_portfolio_accounts (id, plaid_access_token_id, ref_id, balance_available, balance_current, balance_iso_currency_code, balance_limit, mask, name, official_name, subtype, type, profile_id, created_at, updated_at)
values  (1, 1, 'LJy8onGroWTK97vkwxMRuXmZyjomzdtP3bv1K', 100, 110, 'USD', null, '0000', 'Plaid Checking', 'Plaid Gold Standard 0% Interest Checking', 'checking', 'depository', 1, '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (2, 1, 'pvpG9Eby9zIkD5NK4o39Fj38Eaw3N5cLX4yBo', 200, 210, 'USD', null, '1111', 'Plaid Saving', 'Plaid Silver Standard 0.1% Interest Saving', 'savings', 'depository', 1, '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (3, 1, 'ovNA98ZG9rIk1NMq4pBJFK7qkxQ7nNIRvaxge', null, 1000, 'USD', null, '2222', 'Plaid CD', 'Plaid Bronze Standard 0.2% Interest CD', 'cd', 'depository', 1, '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (4, 1, 'goZ1kQwGk8CNbog5dj41CwKM47XKkmSgybazx', null, 410, 'USD', 2000, '3333', 'Plaid Credit Card', 'Plaid Diamond 12.5% APR Interest Credit Card', 'credit card', 'credit', 1, '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (5, 1, '8gVzqa4dq3UrnDaKM7GQia3gqVo3DvhwNA4My', 43200, 43200, 'USD', null, '4444', 'Plaid Money Market', 'Plaid Platinum Standard 1.85% Interest Money Market', 'money market', 'depository', 1, '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (6, 1, 'EJEZvoRpv1TGV6pDLBNZFXM4xAzMdqtX6wvGK', null, 320.76, 'USD', null, '5555', 'Plaid IRA', null, 'ira', 'investment', 1, '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (7, 1, 'W3grzlw7zdi6vp3obZAecRQJMjqQa1tlBrAnr', null, 23631.9805, 'USD', null, '6666', 'Plaid 401k', null, '401k', 'investment', 1, '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (8, 1, 'AJbjpM5ZpeTA4B8Nmx7giMkxNPpkzyh1B7KZ8', null, 65262, 'USD', null, '7777', 'Plaid Student Loan', null, 'student', 'loan', 1, '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (9, 1, 'GJB1o8bgopTMVAjL8nQkc9Ln56pLjrH1Zlrmp', null, 56302.06, 'USD', null, '8888', 'Plaid Mortgage', null, 'mortgage', 'loan', 1, '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00');
ALTER SEQUENCE app.profile_portfolio_accounts_id_seq RESTART WITH 10;

insert into app.portfolio_securities (id, close_price, close_price_as_of, iso_currency_code, name, ref_id, ticker_symbol, type, created_at, updated_at)
values  (9, 28.17, '2021-11-17 00:00:00.000000', 'USD', 'Cambiar International Equity Instl', '3AVe95eyPjHRlGaLdknRsEZ3GM3gq4TGzeM9l', 'CAMYX', 'mutual fund', '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (10, 60585.01953125, '2021-11-18 00:00:00.000000', 'USD', 'Bitcoin', '7Dv19k16PZtvaeloyBgLCxP95o9ynrFggkRaw', 'CUR:BTC', 'cash', '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (11, 0.011, '2021-11-18 00:00:00.000000', 'USD', 'AAPL 06/21/24 225.00 Call', '8E4L9XLl6MudjEpwPAAgivmdZRdBPJuvMPlPb', 'AAPL240621C00225000', 'derivative', '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (12, null, '2021-11-18 00:00:00.000000', 'USD', 'Trp Equity Income', '8E4L9XLl6MurjR94Q1zdSvmdZRdBPJuxyXMBg', null, 'mutual fund', '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (13, 10.45, '2021-11-17 00:00:00.000000', 'USD', 'DoubleLine Total Return Bond I', 'AE5rBXra1AuZLE34rkvvIyG8918m3wtRzElnJ', 'DBLTX', 'mutual fund', '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (15, 1, '2021-11-18 00:00:00.000000', 'USD', 'U S Dollar', 'd6ePmbPxgWCWmMVv66q9iPV94n91vMtov5Are', null, 'cash', '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (17, 13.73, '2021-11-18 00:00:00.000000', 'USD', 'Nh Portfolio 1055 (Fidelity Index)', 'nnmo8doZ4lfKNEDe3mPJipLGkaGw3jfPrpxoN', null, 'etf', '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (18, 38.36, '2021-11-17 00:00:00.000000', 'USD', 'T. Rowe Price Equity Income', 'nnmo8doZ4lfKNpNVerPqCpLGkaGw3jfPrxBP1', 'PRFDX', 'mutual fund', '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (19, 43.98, '2021-11-17 00:00:00.000000', 'USD', 'Southside Bancshares Inc.', 'qy5E8kELlrTnL87xGVjpIBRedAenDzt89pa9q', 'SBSI', 'equity', '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (20, 35, '2021-11-17 00:00:00.000000', 'USD', 'Matthews Pacific Tiger Instl', 'rnXlmPlpqzf3p85Ro9kWugkb86bZpeHLEEd7n', 'MIPTX', 'mutual fund', '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (21, 29.71, '2021-11-17 00:00:00.000000', 'USD', 'iShares MSCI Brazil Index', 'wmDB8ZBW3aUVPnxMbgarinRK7eKzMbfAJJDEJ', 'EWZ', 'etf', '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (22, null, null, 'USD', 'AAPL 06/21/24 160.00 PUT', 'wmDB8ZBW3aUVPnxMbgarinRK7eKzMbfAJJDEI', 'AAPL240621P00160000', 'derivative', '2021-11-12 14:06:51.602102 +00:00', '2021-11-18 05:41:28.397620 +00:00'),
        (16, 4.75, '2021-11-17 00:00:00.000000', 'USD', 'NH Hotel Group S.A.', 'gWwemqe4pntelG7JlBA7tmJVjlVpNoujWQra1', 'NHHEF', 'equity', '2021-11-12 14:06:51.602102 +00:00', '2021-11-19 10:45:21.634671 +00:00'),
        (14, 150, '2021-11-17 00:00:00.000000', 'USD', 'Apple Inc.', 'WDwEPnEmJjt5lp75wl65t1rBmpBZvAHogp7WY', 'AAPL', 'equity', '2021-11-12 14:06:51.602102 +00:00', '2021-11-19 10:45:21.634671 +00:00');
ALTER SEQUENCE app.portfolio_securities_id_seq RESTART WITH 23;

insert into app.profile_holdings (id, plaid_access_token_id, iso_currency_code, quantity, security_id, profile_id, account_id, ref_id, created_at, updated_at)
values  (1, 1, 'USD', 100, 14, 1, 7, 'W3grzlw7zdi6vp3obZAecRQJMjqQa1tlBrAnr_WDwEPnEmJjt5lp75wl65t1rBmpBZvAHogp7WZ', '2021-11-12 14:06:51.602102 +00:00', '2021-11-19 10:45:52.978221 +00:00'),
        (2, 1, 'USD', 3, 22, 1, 7, 'W3grzlw7zdi6vp3obZAecRQJMjqQa1tlBrAnr_wmDB8ZBW3aUVPnxMbgarinRK7eKzMbfAJJDEI', '2021-11-12 14:06:51.602102 +00:00', '2021-11-19 10:45:52.978221 +00:00'),
        (3, 1, 'USD', 0.1, 10, 1, 7, 'W3grzlw7zdi6vp3obZAecRQJMjqQa1tlBrAnr_7Dv19k16PZtvaeloyBgLCxP95o9ynrFggkRaw', '2021-11-12 14:06:51.602102 +00:00', '2021-11-19 10:45:52.978221 +00:00'),
        (4, 1, 'USD', 1000, 15, 1, 7, 'W3grzlw7zdi6vp3obZAecRQJMjqQa1tlBrAnr_d6ePmbPxgWCWmMVv66q9iPV94n91vMtov5Are', '2021-11-12 14:06:51.602102 +00:00', '2021-11-19 10:45:52.978221 +00:00');
ALTER SEQUENCE app.profile_holdings_id_seq RESTART WITH 5;

insert into app.profile_portfolio_transactions (id, plaid_access_token_id, amount, date, fees, iso_currency_code, name, price, quantity, subtype, type, ref_id, security_id, profile_id, account_id, created_at, updated_at)
values  (1,1,  -8.72, '2021-10-21', 0, 'USD', 'INCOME DIV DIVIDEND RECEIVED', 0, 0, 'dividend', 'cash', '6pNKdxJ3dzHArgvoRMQyiVNovq4NJefgzWMNv', 19, 1, 7, '2021-11-18 05:41:24.917136 +00:00', '2021-11-18 05:41:24.917136 +00:00'),
        (2, 1, -1289.01, '2021-10-20', 7.99, 'USD', 'SELL Matthews Pacific Tiger Fund Insti Class', 27.53, -47.74104242992852, 'sell', 'sell', 'vv9KAmbwA4IKkWZdrGPDuRpyn5LprWtWwj4NL', 20, 1, 7, '2021-11-18 05:41:24.917136 +00:00', '2021-11-18 05:41:24.917136 +00:00'),
        (3, 1, -203, '2021-10-20', 3.99, 'USD', 'SELL Trp Equity Income', 20.3, -10, 'sell', 'sell', 'RqXbokJaorHdaXE1enAGf9B8nZrB17HRZKrDm', 12, 1, 7, '2021-11-18 05:41:24.917136 +00:00', '2021-11-18 05:41:24.917136 +00:00'),
        (4, 1, 7.7, '2021-10-19', 7.99, 'USD', 'BUY DoubleLine Total Return Bond Fund', 10.42, 0.7388014749727547, 'buy', 'buy', '9JNBK4WaKxTAwKprGxoEiJ3B5zK3gkuRKLbAb', 13, 1, 7, '2021-11-18 05:41:24.917136 +00:00', '2021-11-18 05:41:24.917136 +00:00'),
        (5, 1, -0.22, '2021-10-18', 5, 'USD', 'SELL Cambiar International Equity Institutional', 25, -0.008902867462305952, 'sell', 'sell', 'ywzB5v9X5jHBqKMNDb3ZHN9BbaL9AKuyg9Ljv', 9, 1, 7, '2021-11-18 05:41:24.917136 +00:00', '2021-11-18 05:41:24.917136 +00:00'),
        (7, 1, 46.32, '2021-10-16', 5, 'USD', 'BUY NFLX DERIVATIVE', 0.01, 4211.152345617756, 'buy', 'buy', 'AJbjpM5ZpeTA4B8Nmx7giMkxNPpkzyh1B7KZy', 11, 1, 6, '2021-11-18 05:41:24.917136 +00:00', '2021-11-18 05:41:24.917136 +00:00'),
        (8, 1, -1200, '2021-10-16', 0, 'USD', 'INVBANKTRAN DEP CO CONTR CURRENT YR EMPLOYER CU CO CONTR CURRENT YR EMPLOYER CUR YR', 0, 0, 'deposit', 'cash', 'GJB1o8bgopTMVAjL8nQkc9Ln56pLjrH1Zlrmq', 15, 1, 6, '2021-11-18 05:41:24.917136 +00:00', '2021-11-18 05:41:24.917136 +00:00'),
        (9, 1, -14961.99, '2021-10-15', 0, 'USD', 'SELL Southside Bancshares Inc.', 34.12, -430.80867509953123, 'sell', 'sell', 'bZQ7gNeGg1IQbq6KrdBMCEvmN6wvAztVB5yaP', 19, 1, 7, '2021-11-18 05:41:24.917136 +00:00', '2021-11-18 05:41:24.917136 +00:00'),
        (10, 1, -2066.58, '2021-10-15', 0, 'USD', 'SELL iShares Inc MSCI Brazil', 41.62, -49.02909689729298, 'sell', 'sell', 'W3grzlw7zdi6vp3obZAecRQJMjqQa1tlBrAn4', 21, 1, 6, '2021-11-18 05:41:24.917136 +00:00', '2021-11-18 05:41:24.917136 +00:00'),
        (11, 1, 120.03, '2021-10-14', 0, 'USD', 'BUY BTC', 40876.02675, 0.00293644, 'buy', 'buy', 'DJjQVpKLVMTbBGRL7oKah4raQA7rplcvZg9b6', 10, 1, 7, '2021-11-18 05:41:24.917136 +00:00', '2021-11-18 05:41:24.917136 +00:00'),
        (12, 1, 466.71, '2021-10-14', 1.95, 'USD', 'BUY FIDELITY INDEX 1055', 13.57, 33.99208384602773, 'buy', 'buy', 'nv1WwgmGwKI8ApVlQLzBFVrzKvnrNpf6JdprJ', 17, 1, 7, '2021-11-18 05:41:24.917136 +00:00', '2021-11-18 05:41:24.917136 +00:00'),
        (13, 1, -0.1, '2021-10-14', 0, 'USD', 'INVBANKTRAN DEP INTEREST EARNED INTEREST EARNED', 0, 0, 'interest', 'cash', 'XxEkBqpeBaH7noqgPaMKtDaVk3oalwtdD4zVB', 15, 1, 7, '2021-11-18 05:41:24.917136 +00:00', '2021-11-18 05:41:24.917136 +00:00'),
        (14, 1, 297.94, '2021-09-10', 1.5, 'USD', 'BUY Apple Inc.', 148.97, 2, 'buy', 'buy', 'bRGUEt49ETnXBk4fktIXxWsldYJ5SRBxhyi22', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (15, 1, 299.1, '2021-09-13', 1.5, 'USD', 'BUY Apple Inc.', 149.55, 2, 'buy', 'buy', 'L5aGxaI8yDPUOkF7UNqJJZN2dxUguH07cUf0w', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (16, 1, 296.24, '2021-09-14', 1.5, 'USD', 'BUY Apple Inc.', 148.12, 2, 'buy', 'buy', 'AIUJqtrnrQc0CY2NWcXFwGI9D7pxzYNxBePBy', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (17, 1, 298.06, '2021-09-15', 1.5, 'USD', 'BUY Apple Inc.', 149.03, 2, 'buy', 'buy', '6E2h3LEjtApe36fzS0kyrW9zkFJaHBSsBTl0O', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (18, 1, 297.58, '2021-09-16', 1.5, 'USD', 'BUY Apple Inc.', 148.79, 2, 'buy', 'buy', 'V71FlxAvKv7l03NY6mvf5FlRxMiUIUh58Gcoj', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (19, 1, 292.12, '2021-09-17', 1.5, 'USD', 'BUY Apple Inc.', 146.06, 2, 'buy', 'buy', 'd8ELQTErNHl3VUFURBfw4KWGbkNq3EKrTgFbF', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (20, 1, 285.88, '2021-09-20', 1.5, 'USD', 'BUY Apple Inc.', 142.94, 2, 'buy', 'buy', 'H5434YMd0cxBujdV2nKXDLlKc7dJ0ZqhiIuYp', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (21, 1, 286.86, '2021-09-21', 1.5, 'USD', 'BUY Apple Inc.', 143.43, 2, 'buy', 'buy', 'NK89ol45vVP7FaRDKFu3gMt2DDidRJNspgIiK', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (22, 1, 291.7, '2021-09-22', 1.5, 'USD', 'BUY Apple Inc.', 145.85, 2, 'buy', 'buy', 'q2EyAMQC3CNPNIGxLvnxaUZG9S9PnY1rg5xS3', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (23, 1, 293.66, '2021-09-23', 1.5, 'USD', 'BUY Apple Inc.', 146.83, 2, 'buy', 'buy', 'zwGDwaITQ72Tzk2RlM9LBxfvQxqg19zAUE1Io', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (24, 1, 293.84, '2021-09-24', 1.5, 'USD', 'BUY Apple Inc.', 146.92, 2, 'buy', 'buy', 'dZJ1HBNsMGhHViyvZTQeNEDHwxXN1bZuLdKvp', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (25, 1, 290.74, '2021-09-27', 1.5, 'USD', 'BUY Apple Inc.', 145.37, 2, 'buy', 'buy', 'GPSt34LkH3l4ljGTCbxshESrAp85bRpoSLoeh', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (26, 1, 283.82, '2021-09-28', 1.5, 'USD', 'BUY Apple Inc.', 141.91, 2, 'buy', 'buy', 'zD8i3dMD5xar82O1SB2PD4J3Psa4oay4EnAlu', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (27, 1, 285.66, '2021-09-29', 1.5, 'USD', 'BUY Apple Inc.', 142.83, 2, 'buy', 'buy', 'a9MwAqrmP0hoytgWyc18VmNKoRrweX083AFNU', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (28, 1, -4245, '2021-09-30', 1.5, 'USD', 'SELL Apple Inc.', 141.5, -30, 'sell', 'sell', 'Jvl3bVpInP3Csq2ymY1H7A5YuF8BWYYZApdN1', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (29, 1, 285.3, '2021-10-01', 1.5, 'USD', 'BUY Apple Inc.', 142.65, 2, 'buy', 'buy', 'JW5laoyMXM4rE8xGQpT5KHS1dmsHfCPi0Lfna', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (30, 1, 278.28, '2021-10-04', 1.5, 'USD', 'BUY Apple Inc.', 139.14, 2, 'buy', 'buy', 'MuSurx2C1CSVANFwcx05ZljREhWJ4bv39xkBS', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (31, 1, 282.22, '2021-10-05', 1.5, 'USD', 'BUY Apple Inc.', 141.11, 2, 'buy', 'buy', 'hrcQ8rNrdbUct6aojcZzfN2NsLxEQZLisVMU3', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (32, 1, 284, '2021-10-06', 1.5, 'USD', 'BUY Apple Inc.', 142, 2, 'buy', 'buy', 'pPVMimH5IRSF43T49Qy7A8z5ZftmGkfSLaPpG', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (33, 1, 286.58, '2021-10-07', 1.5, 'USD', 'BUY Apple Inc.', 143.29, 2, 'buy', 'buy', 'F7Y2aZMOj8tFglT8sqTkSQGc3SLzAimMQV8kk', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (34, 1, 285.8, '2021-10-08', 1.5, 'USD', 'BUY Apple Inc.', 142.9, 2, 'buy', 'buy', '9GqpemEfkNWLrE0b47RH3kyk3tfSUeBffeBVe', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (35, 1, 285.62, '2021-10-11', 1.5, 'USD', 'BUY Apple Inc.', 142.81, 2, 'buy', 'buy', 't8NtZD415xJ6UUcRyiSr3KpMgFNoiuy8AOcjL', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (36, 1, 283.02, '2021-10-12', 1.5, 'USD', 'BUY Apple Inc.', 141.51, 2, 'buy', 'buy', '6qYXXFVCHzeJcUwZYUfiKa6P9PZAAK3XWHxbG', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (37, 1, 281.82, '2021-10-13', 1.5, 'USD', 'BUY Apple Inc.', 140.91, 2, 'buy', 'buy', 'UgwkOZEYbbP11EV0ADoZy2sd2UcnZ6TTMftui', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (38, 1, 287.52, '2021-10-14', 1.5, 'USD', 'BUY Apple Inc.', 143.76, 2, 'buy', 'buy', 'vveH5kiQxp8RAfpnLiFEmOoVCMsU7e3rfHKvH', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (39, 1, -289.68, '2021-10-15', 1.5, 'USD', 'SELL Apple Inc.', 144.84, -2, 'sell', 'sell', 'LqUN9gsNkuedLZ8rfUDexSwj1Ayui0bPoKFSB', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (40, 1, 293.1, '2021-10-18', 1.5, 'USD', 'BUY Apple Inc.', 146.55, 2, 'buy', 'buy', '1r4g1OjJmLUHeHDxPeqhre7hXwc9efxHWs5Fs', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (41, 1, 297.52, '2021-10-19', 1.5, 'USD', 'BUY Apple Inc.', 148.76, 2, 'buy', 'buy', 'NjsuMgE09fvAijw9EptLQuxoJrBnbR5UPfhyX', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (42, 1, 298.52, '2021-10-20', 1.5, 'USD', 'BUY Apple Inc.', 149.26, 2, 'buy', 'buy', 'xajbzgERpmFQ0W2PsAgK8IUq3LMsoZG9eKzbs', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (43, 1, 298.96, '2021-10-21', 1.5, 'USD', 'BUY Apple Inc.', 149.48, 2, 'buy', 'buy', 'K9kdwoIRwFGI7pASk2gQyn4j6dA4hihG8tmWR', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (44, 1, 297.38, '2021-10-22', 1.5, 'USD', 'BUY Apple Inc.', 148.69, 2, 'buy', 'buy', 'sOSHSI4eg674RTsQbFSVtGORC6pbYRGSU4wrA', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (45, 1, 297.28, '2021-10-25', 1.5, 'USD', 'BUY Apple Inc.', 148.64, 2, 'buy', 'buy', 'b3QmrS8KlwCn3M3SKEJqQY6SsRJSbWMQ00FSj', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (46, 1, 298.64, '2021-10-26', 1.5, 'USD', 'BUY Apple Inc.', 149.32, 2, 'buy', 'buy', 'N3tcEibif3rWNOsJRHJKviLUltRxZiM7ZVOep', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (47, 1, 297.7, '2021-10-27', 1.5, 'USD', 'BUY Apple Inc.', 148.85, 2, 'buy', 'buy', 'fOm2uNOGrQiZFkZBcP9Q5xXklroYeITanyJNK', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (48, 1, 305.14, '2021-10-28', 1.5, 'USD', 'BUY Apple Inc.', 152.57, 2, 'buy', 'buy', 'FyWxFMOOq3o9tDXn2OSIXODb2vYqFTeVpHNfk', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (49, 1, 299.6, '2021-10-29', 1.5, 'USD', 'BUY Apple Inc.', 149.8, 2, 'buy', 'buy', 'nnaW38QMLXyg4OdS6NQV6YOVKGZiKN1yLIDKX', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (50, 1, 297.92, '2021-11-01', 1.5, 'USD', 'BUY Apple Inc.', 148.96, 2, 'buy', 'buy', 'nOjwoEOqevaycWB5K8T1o0s6KEkUh23OIux95', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (51, 1, 300.04, '2021-11-02', 1.5, 'USD', 'BUY Apple Inc.', 150.02, 2, 'buy', 'buy', 'q7fJsOnKqrImHvzKVqqX0g7OhlbX3G9Fb2WeB', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (52, 1, 302.98, '2021-11-03', 1.5, 'USD', 'BUY Apple Inc.', 151.49, 2, 'buy', 'buy', 'ahmvdDGcggELOX6NrDJ4Qd9RV5YLm1fwpDnJY', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (53, 1, 301.92, '2021-11-04', 1.5, 'USD', 'BUY Apple Inc.', 150.96, 2, 'buy', 'buy', 'j8UulMIoINK8U0Ot0Iz9o2FvyNCG739DGzTaV', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (54, 1, 302.56, '2021-11-05', 1.5, 'USD', 'BUY Apple Inc.', 151.28, 2, 'buy', 'buy', 'tp01YoIHfCMeImckhNod5nqNkM4SvN8uSwt0q', 14, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (55, 1, 10, '2021-11-06', 1.5, 'USD', 'SELL AAPL 01/14/22 142.00 PUT @ 10', 10, 1, 'sell', 'sell', 'tp01YoIHfCMeImckhNod5nqNkM4SvN8uSwt0w', 22, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00'),
        (56, 1, 10, '2021-11-07', 1.5, 'USD', 'BUY AAPL 01/14/22 142.00 PUT @ 10', 10, 1, 'buy', 'buy', 'tp01YoIHfCMeImckhNod5nqNkM4SvN8uSwt0e', 22, 1, 7, '2021-11-19 13:54:25.993000 +00:00', '2021-11-26 04:07:17.088670 +00:00');
ALTER SEQUENCE app.profile_portfolio_transactions_id_seq RESTART WITH 57;
