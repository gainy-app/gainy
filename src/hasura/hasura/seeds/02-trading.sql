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

insert into app.drivewealth_accounts (ref_id, drivewealth_user_id, trading_account_id, status, ref_no, nickname,
                                      cash_available_for_trade, cash_available_for_withdrawal, cash_balance, data)
values ('bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557', 'bf98c335-57ad-4337-ae9f-ed1fcfb447af', 1, 'OPEN',
        'GYEK000001', 'Mikhail''s Robo Advisor Managed Account', 0, 0, 0,
        '"{\"id\": \"bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557\", \"accountNo\": \"GYEK000001\", \"accountType\": {\"name\": \"LIVE\", \"description\": \"Live Account\"}, \"accountMgmtType\": {\"name\": \"RIA_MANAGED\", \"description\": \"Robo Advisor Managed Account\"}, \"status\": {\"name\": \"OPEN\", \"description\": \"Open\"}, \"tradingType\": {\"name\": \"CASH\", \"description\": \"Cash account\"}, \"leverage\": 1, \"nickname\": \"Mikhail''s Robo Advisor Managed Account\", \"parentIB\": {\"id\": \"7b746acb-0afa-42c3-9c94-1bc8c16ce7b2\", \"name\": \"Gainy\"}, \"taxProfile\": {\"taxStatusCode\": \"W-9\", \"taxRecipientCode\": \"INDIVIDUAL\"}, \"commissionID\": \"4dafc263-f73a-4972-bed0-3af9a6ee3d7d\", \"beneficiaries\": false, \"userID\": \"bf98c335-57ad-4337-ae9f-ed1fcfb447af\", \"restricted\": false, \"goodFaithViolations\": 0, \"patternDayTrades\": 0, \"freeTradeBalance\": 0, \"gfvPdtExempt\": false, \"buyingPowerOverride\": false, \"bod\": {\"moneyMarket\": 0, \"equityValue\": 0, \"cashAvailableForWithdrawal\": 0, \"cashAvailableForTrading\": 0, \"cashBalance\": 0}, \"ria\": {\"advisorID\": \"7b746acb-0afa-42c3-9c94-1bc8c16ce7b2.1661277115494\", \"productID\": \"product_e5046072-eefc-47ed-90d4-60654c33cf92\"}, \"sweepInd\": true, \"interestFree\": false, \"createdWhen\": \"2022-09-05T11:25:45.557Z\", \"openedWhen\": \"2022-09-13T05:29:45.689Z\", \"updatedWhen\": \"2022-09-13T05:29:45.689Z\", \"ignoreMarketHoursForTest\": true, \"flaggedForACATS\": false, \"extendedHoursEnrolled\": false}"')
on conflict do nothing;
ALTER SEQUENCE app.trading_accounts_id_seq RESTART WITH 2;

insert into app.trading_money_flow(id, profile_id, trading_account_id, funding_account_id, status, amount)
values (1, 2, 1, 1, 'Approved', 10000)
on conflict do nothing;
ALTER SEQUENCE app.trading_money_flow_id_seq RESTART WITH 2;

insert into app.drivewealth_deposits(ref_id, trading_account_ref_id, bank_account_ref_id, status, money_flow_id, data)
values ('GYEK000001-1663061386789-DRGPY', 'bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557', 'bank_2101f3e4-ba8c-431e-bfae-aa8070a14fdd', 'Approved', 1, '{"id": "GYEK000001-1663061386789-DRGPY", "paymentID": "GYEK000001-1663061386789-DRGPY", "type": "INSTANT_FUNDING", "amount": 10000.0, "currency": {"name": "USD", "description": "US Dollar", "symbol": "$"}, "status": {"id": 14, "message": "Approved", "comment": "Valid Instant Funding deposit. Auto-move to Approved. Waiting on net settlement.", "updated": "2022-09-13T09:29:46.912Z"}, "accountDetails": {"accountID": "bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557", "accountNo": "GYEK000001", "accountType": {"name": "LIVE", "description": "Live Account"}, "accountManagementType": {"name": "RIA_MANAGED", "description": "Robo Advisor Managed Account"}}, "wlpFinTranTypeID": "50c2100a-2bf8-4a07-91eb-d0395ed15ca9", "timestamp": "2022-09-13T09:29:46.789Z"}')
on conflict do nothing;

insert into app.trading_collection_versions (id, profile_id, collection_id, target_amount_delta, weights)
values  (1, 2, 89, 100, '{"AAPL": "1"}')
on conflict do nothing;
ALTER SEQUENCE app.trading_collection_versions_id_seq RESTART WITH 2;

insert into app.drivewealth_portfolios (ref_id, drivewealth_account_id, holdings, data, cash_target_weight, profile_id)
values  ('portfolio_d7dd65da-96aa-4009-a935-4c2ea5e21130', 'bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557', '{"fund_3dc895e3-a923-4c37-8a91-eac748120215": "0.009900990099009900855098320597"}', '{"id": "portfolio_d7dd65da-96aa-4009-a935-4c2ea5e21130", "name": "Gainy profile #2''s portfolio", "description": "Gainy profile #2''s portfolio", "clientPortfolioID": "2", "holdings": [{"type": "CASH_RESERVE", "target": 1}], "userID": "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2", "triggers": [], "isFundTargetsChanged": false, "fundTargetsChanged": false}', 0.9900990099009900991449016794, 2)
on conflict do nothing;

insert into app.drivewealth_autopilot_runs (ref_id, account_id, status, data, collection_version_id)
values  ('ria_rebalance_f29c337d-e759-4e5e-9715-07c27fb66127', 'bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557', 'SUCCESS', '{"id": "ria_rebalance_f29c337d-e759-4e5e-9715-07c27fb66127", "riaID": "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2", "status": "SUCCESS", "updated": "2022-09-15T14:13:36.885Z", "created": "2022-09-15T14:13:28.980Z", "moneyMovement": false, "regularRebalance": false, "reviewOnly": false, "orders": {"total": 1, "submitted": "https://uat.drivewealth.autopilot.s3.amazonaws.com/7b746acb-0afa-42c3-9c94-1bc8c16ce7b2/2022-09-15T14%3A13%3A28.980Z/orders-submitted.json?X-Amz-Security-Token=IQoJb3JpZ2luX2VjED4aCXVzLWVhc3QtMSJGMEQCIEfK7EOmT3UIhZDuQGYIwO1Xy0OjFBx4tugIIv7q0LZJAiB%2F4cp8FPyFeuRlFFaTjxbBcs42xuc00qlAHfsgQjbw%2FyrWBAjX%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAAaDDY3ODgxOTEwMDI2MyIMvjwGlTSA9y1JE4bRKqoE1IPWpPKkFbtE7rXmRF%2FiAYlJGc3dlC5PKD5y6jspmjVV6eqONQwO9Bbt6JfH%2FXdSUJ9DBS78%2BoYV7EsvmuCq4dFaml4kownPxhgT9SQx0e2VMU51bKNxRHDIMzydpNP1Gbs0t9%2F2DOtzeCON6vURRJhWZ35Q6oIAAPMs2SlhRlA6HrIRxbDpdNX%2FJJH3eNX9VlNSVZp3YWngvSO%2B7QLh6CiSHIGOPTJpbaVrvhhwJcL%2BdTeSn1KY1%2Fio%2B%2FC4FacE3ZPUdJ3xU7jj%2FiXIpCZnEebzu%2BBBjzRY70RkuNkapVPCtzf0Cn8HhRTeW%2BMyvpHdPXrEcbyVuo1cvNmprCO6RENAdxXivkgxGfn9NjOHSEKu%2BWTVYLFfyNy%2Bc5GmSs5SoPkFRXu6Vm4YuEQAm7r%2FtR%2BUkv91wqMctxlzQwuGQ%2BrPWioAgf0p4GrFrxzsoQDh6rmoh6ChTnPS%2Fv5CnZN2N7CdShxX%2BtkEjO2zLz5gCwuujiyZyQSSLuHSWo9cX%2FlV0NHA3Ls0f%2FNZE%2B2wbd4lgDpLppXut5xrjZzy3391IDXTEDZ1lCzd6ypyGF5P6VKn2CgFKvOohGddFpvQ5mZOmfPtn6aWJlh4VfGwjZ0MxD4jV46mo9attQUxsM4KOITyQP10K5yYta4oPTSO1mzCftUZsguM3T8wGWCpwXt2mH9VTFhCjtVa%2BzjxRw0DOx2MOh4HH5TkqVd2KgPCuuXBGhKk7CIn9nCbOM0w2tqMmQY6qgGvT6TK3GBk0FKDWY96AOjn3VqbmlfAB65vi2e0517Dg5vB6hHSwPcQ5%2FGksAVaZrDzt%2FcdEWwYEIWQGS6gSnOW2ocK6NIN%2FRQU81vLXT0NSG%2FooxPsBJP497B49bdUGVwUIs2IeHI6uqNv%2B6h2iWlud%2FhuNv0uzxQKbuCSVG1uUl2%2FEVixZS2a%2FyUW6uVf3Wuq4Jj5XqjFgD%2FY%2Fh%2FCL4ZUyy9sSH89fK8w%2Fg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220915T152301Z&X-Amz-SignedHeaders=host&X-Amz-Expires=1800&X-Amz-Credential=ASIAZ4DGFOZT67PJCUMM%2F20220915%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=1865a64c12e385926c0ac1ed1467bdec67e550de943a56947158842edf617c75", "outcome": "https://uat.drivewealth.autopilot.s3.amazonaws.com/7b746acb-0afa-42c3-9c94-1bc8c16ce7b2/2022-09-15T14%3A13%3A28.980Z/orders-outcome.json?X-Amz-Security-Token=IQoJb3JpZ2luX2VjED4aCXVzLWVhc3QtMSJGMEQCIEfK7EOmT3UIhZDuQGYIwO1Xy0OjFBx4tugIIv7q0LZJAiB%2F4cp8FPyFeuRlFFaTjxbBcs42xuc00qlAHfsgQjbw%2FyrWBAjX%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAAaDDY3ODgxOTEwMDI2MyIMvjwGlTSA9y1JE4bRKqoE1IPWpPKkFbtE7rXmRF%2FiAYlJGc3dlC5PKD5y6jspmjVV6eqONQwO9Bbt6JfH%2FXdSUJ9DBS78%2BoYV7EsvmuCq4dFaml4kownPxhgT9SQx0e2VMU51bKNxRHDIMzydpNP1Gbs0t9%2F2DOtzeCON6vURRJhWZ35Q6oIAAPMs2SlhRlA6HrIRxbDpdNX%2FJJH3eNX9VlNSVZp3YWngvSO%2B7QLh6CiSHIGOPTJpbaVrvhhwJcL%2BdTeSn1KY1%2Fio%2B%2FC4FacE3ZPUdJ3xU7jj%2FiXIpCZnEebzu%2BBBjzRY70RkuNkapVPCtzf0Cn8HhRTeW%2BMyvpHdPXrEcbyVuo1cvNmprCO6RENAdxXivkgxGfn9NjOHSEKu%2BWTVYLFfyNy%2Bc5GmSs5SoPkFRXu6Vm4YuEQAm7r%2FtR%2BUkv91wqMctxlzQwuGQ%2BrPWioAgf0p4GrFrxzsoQDh6rmoh6ChTnPS%2Fv5CnZN2N7CdShxX%2BtkEjO2zLz5gCwuujiyZyQSSLuHSWo9cX%2FlV0NHA3Ls0f%2FNZE%2B2wbd4lgDpLppXut5xrjZzy3391IDXTEDZ1lCzd6ypyGF5P6VKn2CgFKvOohGddFpvQ5mZOmfPtn6aWJlh4VfGwjZ0MxD4jV46mo9attQUxsM4KOITyQP10K5yYta4oPTSO1mzCftUZsguM3T8wGWCpwXt2mH9VTFhCjtVa%2BzjxRw0DOx2MOh4HH5TkqVd2KgPCuuXBGhKk7CIn9nCbOM0w2tqMmQY6qgGvT6TK3GBk0FKDWY96AOjn3VqbmlfAB65vi2e0517Dg5vB6hHSwPcQ5%2FGksAVaZrDzt%2FcdEWwYEIWQGS6gSnOW2ocK6NIN%2FRQU81vLXT0NSG%2FooxPsBJP497B49bdUGVwUIs2IeHI6uqNv%2B6h2iWlud%2FhuNv0uzxQKbuCSVG1uUl2%2FEVixZS2a%2FyUW6uVf3Wuq4Jj5XqjFgD%2FY%2Fh%2FCL4ZUyy9sSH89fK8w%2Fg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220915T152301Z&X-Amz-SignedHeaders=host&X-Amz-Expires=1800&X-Amz-Credential=ASIAZ4DGFOZT67PJCUMM%2F20220915%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=17cf21f07fe6c59a8fa00a501dbaefbe8c540df892972b936fcf1281357c8a2d", "amountBuys": -98.22, "amountSells": 0, "cleanedUp": null}, "allocations": {"submitted": "https://uat.drivewealth.autopilot.s3.amazonaws.com/7b746acb-0afa-42c3-9c94-1bc8c16ce7b2/2022-09-15T14%3A13%3A28.980Z/allocations-submitted.json?X-Amz-Security-Token=IQoJb3JpZ2luX2VjED4aCXVzLWVhc3QtMSJGMEQCIEfK7EOmT3UIhZDuQGYIwO1Xy0OjFBx4tugIIv7q0LZJAiB%2F4cp8FPyFeuRlFFaTjxbBcs42xuc00qlAHfsgQjbw%2FyrWBAjX%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAAaDDY3ODgxOTEwMDI2MyIMvjwGlTSA9y1JE4bRKqoE1IPWpPKkFbtE7rXmRF%2FiAYlJGc3dlC5PKD5y6jspmjVV6eqONQwO9Bbt6JfH%2FXdSUJ9DBS78%2BoYV7EsvmuCq4dFaml4kownPxhgT9SQx0e2VMU51bKNxRHDIMzydpNP1Gbs0t9%2F2DOtzeCON6vURRJhWZ35Q6oIAAPMs2SlhRlA6HrIRxbDpdNX%2FJJH3eNX9VlNSVZp3YWngvSO%2B7QLh6CiSHIGOPTJpbaVrvhhwJcL%2BdTeSn1KY1%2Fio%2B%2FC4FacE3ZPUdJ3xU7jj%2FiXIpCZnEebzu%2BBBjzRY70RkuNkapVPCtzf0Cn8HhRTeW%2BMyvpHdPXrEcbyVuo1cvNmprCO6RENAdxXivkgxGfn9NjOHSEKu%2BWTVYLFfyNy%2Bc5GmSs5SoPkFRXu6Vm4YuEQAm7r%2FtR%2BUkv91wqMctxlzQwuGQ%2BrPWioAgf0p4GrFrxzsoQDh6rmoh6ChTnPS%2Fv5CnZN2N7CdShxX%2BtkEjO2zLz5gCwuujiyZyQSSLuHSWo9cX%2FlV0NHA3Ls0f%2FNZE%2B2wbd4lgDpLppXut5xrjZzy3391IDXTEDZ1lCzd6ypyGF5P6VKn2CgFKvOohGddFpvQ5mZOmfPtn6aWJlh4VfGwjZ0MxD4jV46mo9attQUxsM4KOITyQP10K5yYta4oPTSO1mzCftUZsguM3T8wGWCpwXt2mH9VTFhCjtVa%2BzjxRw0DOx2MOh4HH5TkqVd2KgPCuuXBGhKk7CIn9nCbOM0w2tqMmQY6qgGvT6TK3GBk0FKDWY96AOjn3VqbmlfAB65vi2e0517Dg5vB6hHSwPcQ5%2FGksAVaZrDzt%2FcdEWwYEIWQGS6gSnOW2ocK6NIN%2FRQU81vLXT0NSG%2FooxPsBJP497B49bdUGVwUIs2IeHI6uqNv%2B6h2iWlud%2FhuNv0uzxQKbuCSVG1uUl2%2FEVixZS2a%2FyUW6uVf3Wuq4Jj5XqjFgD%2FY%2Fh%2FCL4ZUyy9sSH89fK8w%2Fg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220915T152302Z&X-Amz-SignedHeaders=host&X-Amz-Expires=1800&X-Amz-Credential=ASIAZ4DGFOZT67PJCUMM%2F20220915%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=859406060a1537bb3b5cbf040241d5af1d8b478ac8994afca23e00a04b628a34", "outcome": "https://uat.drivewealth.autopilot.s3.amazonaws.com/7b746acb-0afa-42c3-9c94-1bc8c16ce7b2/2022-09-15T14%3A13%3A28.980Z/allocations-outcome.json?X-Amz-Security-Token=IQoJb3JpZ2luX2VjED4aCXVzLWVhc3QtMSJGMEQCIEfK7EOmT3UIhZDuQGYIwO1Xy0OjFBx4tugIIv7q0LZJAiB%2F4cp8FPyFeuRlFFaTjxbBcs42xuc00qlAHfsgQjbw%2FyrWBAjX%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAAaDDY3ODgxOTEwMDI2MyIMvjwGlTSA9y1JE4bRKqoE1IPWpPKkFbtE7rXmRF%2FiAYlJGc3dlC5PKD5y6jspmjVV6eqONQwO9Bbt6JfH%2FXdSUJ9DBS78%2BoYV7EsvmuCq4dFaml4kownPxhgT9SQx0e2VMU51bKNxRHDIMzydpNP1Gbs0t9%2F2DOtzeCON6vURRJhWZ35Q6oIAAPMs2SlhRlA6HrIRxbDpdNX%2FJJH3eNX9VlNSVZp3YWngvSO%2B7QLh6CiSHIGOPTJpbaVrvhhwJcL%2BdTeSn1KY1%2Fio%2B%2FC4FacE3ZPUdJ3xU7jj%2FiXIpCZnEebzu%2BBBjzRY70RkuNkapVPCtzf0Cn8HhRTeW%2BMyvpHdPXrEcbyVuo1cvNmprCO6RENAdxXivkgxGfn9NjOHSEKu%2BWTVYLFfyNy%2Bc5GmSs5SoPkFRXu6Vm4YuEQAm7r%2FtR%2BUkv91wqMctxlzQwuGQ%2BrPWioAgf0p4GrFrxzsoQDh6rmoh6ChTnPS%2Fv5CnZN2N7CdShxX%2BtkEjO2zLz5gCwuujiyZyQSSLuHSWo9cX%2FlV0NHA3Ls0f%2FNZE%2B2wbd4lgDpLppXut5xrjZzy3391IDXTEDZ1lCzd6ypyGF5P6VKn2CgFKvOohGddFpvQ5mZOmfPtn6aWJlh4VfGwjZ0MxD4jV46mo9attQUxsM4KOITyQP10K5yYta4oPTSO1mzCftUZsguM3T8wGWCpwXt2mH9VTFhCjtVa%2BzjxRw0DOx2MOh4HH5TkqVd2KgPCuuXBGhKk7CIn9nCbOM0w2tqMmQY6qgGvT6TK3GBk0FKDWY96AOjn3VqbmlfAB65vi2e0517Dg5vB6hHSwPcQ5%2FGksAVaZrDzt%2FcdEWwYEIWQGS6gSnOW2ocK6NIN%2FRQU81vLXT0NSG%2FooxPsBJP497B49bdUGVwUIs2IeHI6uqNv%2B6h2iWlud%2FhuNv0uzxQKbuCSVG1uUl2%2FEVixZS2a%2FyUW6uVf3Wuq4Jj5XqjFgD%2FY%2Fh%2FCL4ZUyy9sSH89fK8w%2Fg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220915T152302Z&X-Amz-SignedHeaders=host&X-Amz-Expires=1800&X-Amz-Credential=ASIAZ4DGFOZT67PJCUMM%2F20220915%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=6304c109a0e2ef36c2c4061a7592fa5ec195a0d2e7625e5bdbe5ffe5226efdd6"}, "subAccounts": {"total": 1, "rebalanced": 1, "error": 0, "outcome": "https://uat.drivewealth.autopilot.s3.amazonaws.com/7b746acb-0afa-42c3-9c94-1bc8c16ce7b2/2022-09-15T14%3A13%3A28.980Z/sub-accounts-outcome.json?X-Amz-Security-Token=IQoJb3JpZ2luX2VjED4aCXVzLWVhc3QtMSJGMEQCIEfK7EOmT3UIhZDuQGYIwO1Xy0OjFBx4tugIIv7q0LZJAiB%2F4cp8FPyFeuRlFFaTjxbBcs42xuc00qlAHfsgQjbw%2FyrWBAjX%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAAaDDY3ODgxOTEwMDI2MyIMvjwGlTSA9y1JE4bRKqoE1IPWpPKkFbtE7rXmRF%2FiAYlJGc3dlC5PKD5y6jspmjVV6eqONQwO9Bbt6JfH%2FXdSUJ9DBS78%2BoYV7EsvmuCq4dFaml4kownPxhgT9SQx0e2VMU51bKNxRHDIMzydpNP1Gbs0t9%2F2DOtzeCON6vURRJhWZ35Q6oIAAPMs2SlhRlA6HrIRxbDpdNX%2FJJH3eNX9VlNSVZp3YWngvSO%2B7QLh6CiSHIGOPTJpbaVrvhhwJcL%2BdTeSn1KY1%2Fio%2B%2FC4FacE3ZPUdJ3xU7jj%2FiXIpCZnEebzu%2BBBjzRY70RkuNkapVPCtzf0Cn8HhRTeW%2BMyvpHdPXrEcbyVuo1cvNmprCO6RENAdxXivkgxGfn9NjOHSEKu%2BWTVYLFfyNy%2Bc5GmSs5SoPkFRXu6Vm4YuEQAm7r%2FtR%2BUkv91wqMctxlzQwuGQ%2BrPWioAgf0p4GrFrxzsoQDh6rmoh6ChTnPS%2Fv5CnZN2N7CdShxX%2BtkEjO2zLz5gCwuujiyZyQSSLuHSWo9cX%2FlV0NHA3Ls0f%2FNZE%2B2wbd4lgDpLppXut5xrjZzy3391IDXTEDZ1lCzd6ypyGF5P6VKn2CgFKvOohGddFpvQ5mZOmfPtn6aWJlh4VfGwjZ0MxD4jV46mo9attQUxsM4KOITyQP10K5yYta4oPTSO1mzCftUZsguM3T8wGWCpwXt2mH9VTFhCjtVa%2BzjxRw0DOx2MOh4HH5TkqVd2KgPCuuXBGhKk7CIn9nCbOM0w2tqMmQY6qgGvT6TK3GBk0FKDWY96AOjn3VqbmlfAB65vi2e0517Dg5vB6hHSwPcQ5%2FGksAVaZrDzt%2FcdEWwYEIWQGS6gSnOW2ocK6NIN%2FRQU81vLXT0NSG%2FooxPsBJP497B49bdUGVwUIs2IeHI6uqNv%2B6h2iWlud%2FhuNv0uzxQKbuCSVG1uUl2%2FEVixZS2a%2FyUW6uVf3Wuq4Jj5XqjFgD%2FY%2Fh%2FCL4ZUyy9sSH89fK8w%2Fg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220915T152302Z&X-Amz-SignedHeaders=host&X-Amz-Expires=1800&X-Amz-Credential=ASIAZ4DGFOZT67PJCUMM%2F20220915%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=72c2b2d79c59006246c8747e68ad4dbcc0bd5084e3c2adc9cf9748251785611d"}}', 1)
on conflict do nothing;

insert into app.drivewealth_funds (ref_id, profile_id, collection_id, trading_collection_version_id, holdings, weights, data)
values  ('fund_3dc895e3-a923-4c37-8a91-eac748120215', 2, 89, 1, '[{"instrumentID": "a67422af-8504-43df-9e63-7361eb0bd99e", "target": "1"}]', '{"AAPL": "1"}', '{"id": "fund_3dc895e3-a923-4c37-8a91-eac748120215", "userID": "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2", "name": "Gainy bf98c335-57ad-4337-ae9f-ed1fcfb447af''s fund for collection 89", "type": "FUND", "clientFundID": "2_89", "description": "Gainy bf98c335-57ad-4337-ae9f-ed1fcfb447af''s fund for collection 89", "holdings": [{"instrumentID": "a67422af-8504-43df-9e63-7361eb0bd99e", "target": 1}], "triggers": [], "isInstrumentTargetsChanged": false, "instrumentTargetsChanged": false}')
on conflict do nothing;
