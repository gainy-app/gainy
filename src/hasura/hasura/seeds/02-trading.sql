INSERT INTO app.kyc_form (first_name, last_name, country, phone_number, email_address, language, employment_status,
                          employment_company_name, employment_type, employment_position,
                          employment_affiliated_with_a_broker, employment_is_director_of_a_public_company,
                          investor_profile_experience, investor_profile_annual_income, investor_profile_net_worth_total,
                          investor_profile_risk_tolerance, investor_profile_objectives,
                          investor_profile_net_worth_liquid, disclosures_drivewealth_terms_of_use,
                          disclosures_drivewealth_customer_agreement, disclosures_drivewealth_ira_agreement,
                          disclosures_drivewealth_market_data_agreement, disclosures_rule14b,
                          disclosures_drivewealth_privacy_policy, disclosures_drivewealth_data_sharing,
                          disclosures_signed_by, disclosures_extended_hours_agreement, tax_id_value, tax_id_type,
                          citizenship, is_us_tax_payer, tax_treaty_with_us, birthdate, politically_exposed_names,
                          irs_backup_withholdings_notified, gender, marital_status, address_street1, address_street2,
                          address_city, address_province, address_postal_code, address_country, status, profile_id)
VALUES ('Mikhail', 'Astashkevich', 'USA', '+1234567890', 'test@gainy.app', null, 'UNEMPLOYED', null, null, null,
        null, null, 'YRS_10_', 123456, 1234, 'SPECULATION', 'LONG_TERM', 123, true, true, null, true, true, true, null,
        'Mikhail Astashkevich', null, '123456789', 'SSN', 'USA', null, null, '1992-11-27', null, null, null, null,
        '1 Wall st.', null, 'New York', 'CA', '12345', 'USA', 'APPROVED', 2)
on conflict do nothing;

INSERT INTO app.drivewealth_users (ref_id, profile_id, status, data)
VALUES ('bf98c335-57ad-4337-ae9f-ed1fcfb447af', 2, 'APPROVED',
        '"{\"id\": \"bf98c335-57ad-4337-ae9f-ed1fcfb447af\", \"ackSignedWhen\": \"2022-09-13T05:29:44.416Z\", \"addressLine1\": \"1 Wall st.\", \"addressLine2\": null, \"city\": \"New York\", \"countryID\": \"USA\", \"displayName\": \"MAstashkevich\", \"dob\": \"1992-11-27\", \"email\": \"qurazor1@gmail.com\", \"firstName\": \"Mikhail\", \"languageID\": \"en_US\", \"lastName\": \"Astashkevich\", \"parentIB\": {\"id\": \"7b746acb-0afa-42c3-9c94-1bc8c16ce7b2\", \"name\": \"Gainy\"}, \"phone\": \"+1234567890\", \"referralCode\": \"78AA48\", \"stateProvince\": \"CA\", \"wlpID\": \"GAIN\", \"zipPostalCode\": \"12345\", \"idNo\": \"****6789\", \"status\": {\"name\": \"APPROVED\", \"description\": \"User approved.\"}, \"userType\": {\"name\": \"INDIVIDUAL_TRADER\", \"description\": \"Individual Trader\"}, \"usCitizen\": false, \"updatedWhen\": \"2022-09-13T05:29:45.795Z\", \"brandAmbassador\": false, \"employmentStatus\": {\"name\": \"UNEMPLOYED\", \"description\": \"Not Employed / In between jobs\"}, \"citizenship\": \"USA\", \"createdWhen\": \"2022-09-05T11:25:43.363Z\", \"approvedWhen\": \"2022-09-13T05:29:45.795Z\", \"approvedBy\": \"KYC SYSTEM\", \"marginDefault\": 0, \"ackCustomerAgreement\": true, \"ackFindersFee\": false, \"ackForeignFindersFee\": false, \"ackJointCustomerAgreement\": false, \"ackJointFindersFee\": false, \"ackJointForeignFindersFee\": false, \"ackJointMarketData\": false, \"ackMarketData\": true, \"ackSignedBy\": \"Mikhail Astashkevich\", \"ackExtendedHoursAgreement\": false, \"termsOfUse\": true, \"badPasswordCount\": 0, \"director\": false, \"employerIsBroker\": false, \"employmentYears\": 0, \"jointEmployerIsBroker\": false, \"investmentObjectives\": {\"name\": \"LONG_TERM\", \"description\": \"Long-term buy & hold investing\"}, \"investmentExperience\": {\"name\": \"YRS_10_\", \"description\": \"10+ yrs\"}, \"fundingSources\": [], \"politicallyExposed\": false, \"riskTolerance\": \"Speculative Risk\", \"userNoteQty\": 0, \"w8received\": \"2022-09-13\", \"w8expires\": \"2025-12-31\", \"taxTreatyWithUS\": false, \"updatedBy\": \"KYC_SYSTEM\", \"avatarURL\": \"https://secure.gravatar.com/avatar/6c2e4e3d706e25a4935f4682bc55c51c.jpg\", \"annualIncomeRange\": \"$100,000 - $199,999\", \"ackDisclosureRule14b\": true, \"ackJointDisclosureRule14b\": false, \"networthLiquidRange\": \"$0 - $4,999\", \"networthTotalRange\": \"$0 - $4,999\"}"')
on conflict do nothing;

INSERT INTO app.profile_plaid_access_tokens (profile_id, access_token, item_id, purpose)
select t.profile_id,
       t.access_token,
       t.item_id,
       t.purpose
from (
         select 2 as                                                     profile_id,
                'access-sandbox-b7033717-380d-4a40-a0a9-acfee365eaea' as access_token,
                'lK3VVvnW7ZH9Bdnqe11RI86KLXZDNDiZA6vpy' as               item_id,
                'trading' as                                             purpose
     ) t
         left join app.profile_plaid_access_tokens using (profile_id, purpose)
where profile_plaid_access_tokens is null;

INSERT INTO app.trading_funding_accounts (id, profile_id, plaid_access_token_id, plaid_account_id, name, balance)
select 1,
       2,
       profile_plaid_access_tokens.id,
       'B6XxxGblwRt4BMRZNgg1Ul6wvwzELwFZbKJkb',
       'Plaid Saving',
       null
from app.profile_plaid_access_tokens
where profile_id = 2
  and purpose = 'trading'
on conflict do nothing;
ALTER SEQUENCE app.trading_funding_accounts_id_seq RESTART WITH 2;


INSERT INTO app.drivewealth_bank_accounts (ref_id, drivewealth_user_id, funding_account_id, plaid_access_token_id,
                                           plaid_account_id, status, bank_account_nickname, bank_account_number,
                                           bank_routing_number, holder_name, bank_account_type, data)
select 'bank_2101f3e4-ba8c-431e-bfae-aa8070a14fdd',
       'bf98c335-57ad-4337-ae9f-ed1fcfb447af',
       1,
       profile_plaid_access_tokens.id,
       'B6XxxGblwRt4BMRZNgg1Ul6wvwzELwFZbKJkb',
       'ACTIVE',
       'Plaid Saving',
       '****1111',
       '****1533',
       'Mikhail Astashkevich',
       null,
       '"{\"id\": \"bank_2101f3e4-ba8c-431e-bfae-aa8070a14fdd\", \"userDetails\": {\"userID\": \"bf98c335-57ad-4337-ae9f-ed1fcfb447af\", \"username\": \"gainba49344340cd4288b6a98557d2ae566d\", \"firstName\": \"Mikhail\", \"lastName\": \"Astashkevich\", \"email\": \"qurazor1@gmail.com\", \"parentIB\": {\"id\": \"7b746acb-0afa-42c3-9c94-1bc8c16ce7b2\", \"name\": \"Gainy\"}, \"wlpID\": \"GAIN\"}, \"bankAccountDetails\": {\"bankAccountNickname\": \"Plaid Saving\", \"bankAccountNumber\": \"****1111\", \"bankRoutingNumber\": \"****1533\"}, \"status\": \"ACTIVE\", \"created\": \"2022-09-05T11:33:11.934Z\", \"updated\": \"2022-09-05T11:33:11.934Z\"}"'
from app.profile_plaid_access_tokens
where profile_id = 2
  and purpose = 'trading';


INSERT INTO app.trading_accounts (id, profile_id, name, cash_available_for_trade, cash_available_for_withdrawal,
                                  cash_balance)
VALUES (1, 2, 'Mikhail''s Robo Advisor Managed Account', 0, 0, 0)
on conflict do nothing;
ALTER SEQUENCE app.trading_accounts_id_seq RESTART WITH 2;

insert into app.drivewealth_accounts (ref_id, drivewealth_user_id, trading_account_id, status, ref_no, nickname,
                                      cash_available_for_trade, cash_available_for_withdrawal, cash_balance, data)
values ('bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557', 'bf98c335-57ad-4337-ae9f-ed1fcfb447af', 1, 'OPEN',
        'GYEK000001', 'Mikhail''s Robo Advisor Managed Account', 0, 0, 0,
        '{"id": "bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557", "accountNo": "GYEK000001", "accountType": {"name": "LIVE", "description": "Live Account"}, "accountMgmtType": {"name": "RIA_MANAGED", "description": "Robo Advisor Managed Account"}, "status": {"name": "OPEN", "description": "Open"}, "tradingType": {"name": "CASH", "description": "Cash account"}, "leverage": 1, "nickname": "Mikhail''s Robo Advisor Managed Account", "parentIB": {"id": "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2", "name": "Gainy"}, "taxProfile": {"taxStatusCode": "W-9", "taxRecipientCode": "INDIVIDUAL"}, "commissionID": "4dafc263-f73a-4972-bed0-3af9a6ee3d7d", "beneficiaries": false, "userID": "bf98c335-57ad-4337-ae9f-ed1fcfb447af", "restricted": false, "goodFaithViolations": 0, "patternDayTrades": 0, "freeTradeBalance": 0, "gfvPdtExempt": false, "buyingPowerOverride": false, "bod": {"moneyMarket": 0, "equityValue": 0, "cashAvailableForWithdrawal": 0, "cashAvailableForTrading": 0, "cashBalance": 0}, "ria": {"advisorID": "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2.1661277115494", "productID": "product_e5046072-eefc-47ed-90d4-60654c33cf92"}, "sweepInd": true, "interestFree": false, "createdWhen": "2022-09-05T11:25:45.557Z", "openedWhen": "2022-09-13T05:29:45.689Z", "updatedWhen": "2022-09-13T05:29:45.689Z", "ignoreMarketHoursForTest": true, "flaggedForACATS": false, "extendedHoursEnrolled": false}')
on conflict do nothing;

insert into app.trading_money_flow(id, profile_id, trading_account_id, funding_account_id, status, amount)
values (1, 2, 1, 1, 'SUCCESS', 30000)
on conflict do nothing;
ALTER SEQUENCE app.trading_money_flow_id_seq RESTART WITH 2;

insert into app.drivewealth_deposits(ref_id, trading_account_ref_id, bank_account_ref_id, status, data)
values ('GYEK000001-1663061386789-DRGPY', 'bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557', 'bank_2101f3e4-ba8c-431e-bfae-aa8070a14fdd', 'Approved', '{"id": "GYEK000001-1663061386789-DRGPY", "paymentID": "GYEK000001-1663061386789-DRGPY", "type": "INSTANT_FUNDING", "amount": 10000.0, "currency": {"name": "USD", "description": "US Dollar", "symbol": "$"}, "status": {"id": 14, "message": "Approved", "comment": "Valid Instant Funding deposit. Auto-move to Approved. Waiting on net settlement.", "updated": "2022-09-13T09:29:46.912Z"}, "accountDetails": {"accountID": "bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557", "accountNo": "GYEK000001", "accountType": {"name": "LIVE", "description": "Live Account"}, "accountManagementType": {"name": "RIA_MANAGED", "description": "Robo Advisor Managed Account"}}, "wlpFinTranTypeID": "50c2100a-2bf8-4a07-91eb-d0395ed15ca9", "timestamp": "2022-09-13T09:29:46.789Z"}')
on conflict do nothing;

-- Portfolio
insert into app.drivewealth_portfolios (ref_id, profile_id, drivewealth_account_id, holdings, data, cash_target_weight)
values  ('portfolio_d7dd65da-96aa-4009-a935-4c2ea5e21130', 2, 'bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557', '{"fund_3dc895e3-a923-4c37-8a91-eac748120215": 0.0061}', '{"id": "portfolio_d7dd65da-96aa-4009-a935-4c2ea5e21130", "name": "Gainy profile #2''s portfolio", "description": "Gainy profile #2''s portfolio", "clientPortfolioID": "2", "holdings": [{"type": "CASH_RESERVE", "target": 0.9939}, {"id": "fund_3dc895e3-a923-4c37-8a91-eac748120215", "name": "Gainy bf98c335-57ad-4337-ae9f-ed1fcfb447af''s fund for collection 89", "type": "FUND", "clientFundID": "2_89", "description": "Gainy bf98c335-57ad-4337-ae9f-ed1fcfb447af''s fund for collection 89", "target": 0.0061, "holdings": [{"instrumentID": "a67422af-8504-43df-9e63-7361eb0bd99e", "target": 1}], "triggers": []}], "userID": "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2", "triggers": [], "isFundTargetsChanged": false, "fundTargetsChanged": false}', 0.9939)
on conflict do nothing;

insert into app.drivewealth_portfolio_statuses (drivewealth_portfolio_id, cash_value, cash_actual_weight, data)
values  ('portfolio_d7dd65da-96aa-4009-a935-4c2ea5e21130', 29758.9300000000002910383045673370361328125, 0.993700000000000027711166694643907248973846435546875, '{"id": "portfolio_d7dd65da-96aa-4009-a935-4c2ea5e21130", "equity": 29946.88, "totalDrift": 0.0001, "rebalanceRequired": false, "nextPortfolioRebalance": "2022-10-18T13:30:00.000Z", "lastPortfolioRebalance": "2022-10-17T13:32:17.053Z", "holdings": [{"id": "CASH", "type": "CASH_RESERVE", "name": null, "target": 0.9938, "actual": 0.9937, "value": 29758.93, "rebalanceRequired": false, "holdings": null}, {"id": "fund_3dc895e3-a923-4c37-8a91-eac748120215", "type": "FUND", "name": "Gainy bf98c335-57ad-4337-ae9f-ed1fcfb447af''s fund for collection 89", "target": 0.006, "actual": 0.0062, "value": 187.95, "rebalanceRequired": false, "holdings": [{"instrumentID": "a67422af-8504-43df-9e63-7361eb0bd99e", "symbol": "AAPL", "target": 1, "actual": 1.0, "openQty": 1.31979237, "value": 187.95}]}]}');

insert into app.trading_collection_versions (id, profile_id, collection_id, status, target_amount_delta, weights, executed_at)
values  (1, 2, 83, 'EXECUTED_FULLY', 200, '{"AAPL": "1"}', '2022-10-17 13:29:20.422333 +00:00'),
        (2, 2, 83, null, -1000, '{"AAPL": "1"}', null),
        (3, 2, 83, null, -1000, '{"AAPL": "1"}', null),
        (4, 2, 83, null, -1000, '{"AAPL": "1"}', null),
        (5, 2, 83, 'EXECUTED_FULLY', -1000, '{"AAPL": "1"}', '2022-10-17 13:29:21.655114 +00:00'),
        (6, 2, 83, 'EXECUTED_FULLY', 1000, '{"AAPL": "1"}', '2022-09-14 13:36:42.758000 +00:00')
on conflict do nothing;
ALTER SEQUENCE app.trading_collection_versions_id_seq RESTART WITH 7;

insert into app.drivewealth_instruments (ref_id, symbol, status, data)
values  ('a67422af-8504-43df-9e63-7361eb0bd99e', 'AAPL', 'ACTIVE', '{"symbol": "AAPL", "reutersPrimaryRic": "AAPL.O", "name": "Apple", "description": "Apple Inc. (Apple) designs, manufactures and markets smartphones, personal computers, tablets, wearables and accessories and sells a range of related services. The Company\u2019s products include iPhone, Mac, iPad, AirPods, Apple TV, Apple Watch, Beats products, HomePod, iPod touch and accessories. The Company operates various platforms, including the App Store, which allows customers to discover and download applications and digital content, such as books, music, video, games and podcasts. Apple offers digital content through subscription-based services, including Apple Arcade, Apple Music, Apple News+, Apple TV+ and Apple Fitness+. Apple also offers a range of other services, such as AppleCare, iCloud, Apple Card and Apple Pay. Apple sells its products and resells third-party products in a range of markets, including directly to consumers, small and mid-sized businesses, and education, enterprise and government customers through its retail and online stores and its direct sales force.", "sector": "Technology", "longOnly": true, "orderSizeMax": 10000, "orderSizeMin": 1e-08, "orderSizeStep": 1e-08, "exchangeNickelSpread": false, "close": 0, "descriptionChinese": "Apple Inc\u8bbe\u8ba1\u3001\u5236\u9020\u548c\u9500\u552e\u667a\u80fd\u624b\u673a\u3001\u4e2a\u4eba\u7535\u8111\u3001\u5e73\u677f\u7535\u8111\u3001\u53ef\u7a7f\u6234\u8bbe\u5907\u548c\u914d\u4ef6\uff0c\u5e76\u63d0\u4f9b\u5404\u79cd\u76f8\u5173\u670d\u52a1\u3002\u8be5\u516c\u53f8\u7684\u4ea7\u54c1\u5305\u62eciPhone\u3001Mac\u3001iPad\u4ee5\u53ca\u53ef\u7a7f\u6234\u8bbe\u5907\u3001\u5bb6\u5c45\u548c\u914d\u4ef6\u3002iPhone\u662f\u8be5\u516c\u53f8\u57fa\u4e8eiOS\u64cd\u4f5c\u7cfb\u7edf\u7684\u667a\u80fd\u624b\u673a\u7cfb\u5217\u3002Mac\u662f\u8be5\u516c\u53f8\u57fa\u4e8emacOS\u64cd\u4f5c\u7cfb\u7edf\u7684\u4e2a\u4eba\u7535\u8111\u7cfb\u5217\u3002iPad\u662f\u8be5\u516c\u53f8\u57fa\u4e8eiPadOS\u64cd\u4f5c\u7cfb\u7edf\u7684\u591a\u529f\u80fd\u5e73\u677f\u7535\u8111\u7cfb\u5217\u3002\u53ef\u7a7f\u6234\u8bbe\u5907\u3001\u5bb6\u5c45\u548c\u914d\u4ef6\u5305\u62ecAirPods\u3001Apple TV\u3001Apple Watch\u3001Beats\u4ea7\u54c1\u3001HomePod\u3001iPod touch\u548c\u5176\u4ed6Apple\u54c1\u724c\u53ca\u7b2c\u4e09\u65b9\u914d\u4ef6\u3002AirPods\u662f\u8be5\u516c\u53f8\u53ef\u4ee5\u4e0eSiri\u4ea4\u4e92\u7684\u65e0\u7ebf\u8033\u673a\u3002Apple Watch\u662f\u8be5\u516c\u53f8\u7684\u667a\u80fd\u624b\u8868\u7cfb\u5217\u3002\u5176\u670d\u52a1\u5305\u62ec\u5e7f\u544a\u3001AppleCare\u3001\u4e91\u670d\u52a1\u3001\u6570\u5b57\u5185\u5bb9\u548c\u652f\u4ed8\u670d\u52a1\u3002\u5176\u5ba2\u6237\u4e3b\u8981\u6765\u81ea\u6d88\u8d39\u8005\u3001\u4e2d\u5c0f\u4f01\u4e1a\u3001\u6559\u80b2\u3001\u4f01\u4e1a\u548c\u653f\u5e9c\u5e02\u573a\u3002 ", "id": "a67422af-8504-43df-9e63-7361eb0bd99e", "type": "EQUITY", "exchange": "NSQ", "url": "http://investor.apple.com", "status": "ACTIVE", "closePrior": 142.99, "image": "https://uat-drivewealth.imgix.net/symbols/aapl.png?fit=fillmax&w=125&h=125&bg=FFFFFF", "ISIN": "US0378331005"}')
on conflict do nothing;

insert into app.drivewealth_funds (ref_id, profile_id, collection_id, trading_collection_version_id, holdings, weights, data)
values  ('fund_3dc895e3-a923-4c37-8a91-eac748120215', 2, 83, 1, '[{"instrumentID": "a67422af-8504-43df-9e63-7361eb0bd99e", "target": "1"}]', '{"AAPL": "1"}', '{"id": "fund_3dc895e3-a923-4c37-8a91-eac748120215", "userID": "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2", "name": "Gainy bf98c335-57ad-4337-ae9f-ed1fcfb447af''s fund for collection 89", "type": "FUND", "clientFundID": "2_89", "description": "Gainy bf98c335-57ad-4337-ae9f-ed1fcfb447af''s fund for collection 89", "holdings": [{"instrumentID": "a67422af-8504-43df-9e63-7361eb0bd99e", "target": 1}], "triggers": [], "isInstrumentTargetsChanged": false, "instrumentTargetsChanged": false}')
on conflict do nothing;

insert into app.drivewealth_accounts_positions (id, drivewealth_account_id, equity_value, data, created_at)
select row_number() over (),
       'bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557',
       1000 + (random() - 0.5) * 300,
       null,
       dd
from generate_series(now() - interval '2 months', now(), interval '1 day') dd
on conflict do nothing;
select setval('app.drivewealth_accounts_positions_id_seq', (select max(id)+1 from app.drivewealth_accounts_positions), false);

-- Commissions
insert into app.payment_methods (id, profile_id, name, provider)
values  (1, 2, 'card visa 4242', 'STRIPE')
on conflict do nothing;
ALTER SEQUENCE app.payment_methods_id_seq RESTART WITH 2;

insert into app.stripe_payment_methods (ref_id, customer_ref_id, payment_method_id, name, data)
values  ('pm_1LhEWuD1LH0kYxao7AOiSejL', 'cus_MQ4gYGmV5H4WcR', 1, 'card visa 4242', '{"id": "pm_1LhEWuD1LH0kYxao7AOiSejL", "object": "payment_method", "billing_details": {"address": {"city": null, "country": null, "line1": null, "line2": null, "postal_code": null, "state": null}, "email": null, "name": null, "phone": null}, "card": {"brand": "visa", "checks": {"address_line1_check": null, "address_postal_code_check": null, "cvc_check": null}, "country": "US", "exp_month": 9, "exp_year": 2023, "fingerprint": "4iC4R4MJWP7lXxKu", "funding": "credit", "generated_from": null, "last4": "4242", "networks": {"available": ["visa"], "preferred": null}, "three_d_secure_usage": {"supported": true}, "wallet": null}, "created": 1662996396, "customer": "cus_MQ4gYGmV5H4WcR", "livemode": false, "metadata": {}, "type": "card"}')
on conflict do nothing;
