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
VALUES ('Mikhail', 'Astashkevich', null, '+1234567890', 'test@gainy.app', null, 'UNEMPLOYED', null, null, null,
        null, null, 'YRS_10_', 123456, 1234, 'SPECULATION', 'LONG_TERM', 123, true, true, null, true, true, true, null,
        'Mikhail Astashkevich', null, '123456789', 'SSN', null, null, null, '1992-11-27', null, null, null, null,
        '1 Wall st.', null, 'New York', null, '12345', null, 'PENDING', 2)
on conflict do nothing;

INSERT INTO app.drivewealth_users (ref_id, profile_id, status, data)
VALUES ('bf98c335-57ad-4337-ae9f-ed1fcfb447af', 2, 'PENDING',
        '"{\"id\": \"bf98c335-57ad-4337-ae9f-ed1fcfb447af\", \"userType\": {\"name\": \"INDIVIDUAL_TRADER\", \"description\": \"Individual Trader\"}, \"status\": {\"name\": \"PENDING\", \"description\": \"User is pending approval.\"}, \"parentIB\": {\"id\": \"7b746acb-0afa-42c3-9c94-1bc8c16ce7b2\", \"name\": \"Gainy\"}, \"documents\": [{\"type\": \"ADDRESS_INFO\", \"data\": {\"street1\": \"1 Wall st.\", \"city\": \"New York\", \"postalCode\": \"12345\", \"country\": \"USA\"}, \"description\": \"Physical address information\"}, {\"type\": \"BASIC_INFO\", \"data\": {\"firstName\": \"Mikhail\", \"lastName\": \"Astashkevich\", \"displayName\": \"MAstashkevich\", \"emailAddress\": \"qurazor1@gmail.com\", \"phone\": \"+1234567890\", \"country\": \"USA\", \"language\": \"en_US\"}, \"description\": \"Name, email, phone, etc.\"}, {\"type\": \"DISCLOSURES\", \"data\": {\"termsOfUse\": true, \"rule14b\": true, \"customerAgreement\": true, \"findersFee\": false, \"foreignFindersFee\": false, \"marketDataAgreement\": true, \"privacyPolicy\": true, \"dataSharing\": null, \"signedBy\": \"Mikhail Astashkevich\", \"signedWhen\": \"2022-09-05T11:25:43.296Z\", \"iraAgreement\": null, \"extendedHoursAgreement\": false, \"cryptoAgreements\": null}, \"description\": \"Agreement acknowledgements\"}, {\"type\": \"EMPLOYMENT_INFO\", \"data\": {\"status\": \"UNEMPLOYED\", \"broker\": false, \"years\": 0}, \"description\": \"Employer, position, etc.\"}, {\"type\": \"IDENTIFICATION_INFO\", \"data\": {\"value\": \"****6789\", \"type\": \"Social Security Number\", \"citizenship\": \"USA\"}, \"description\": \"ID Number and citizenship\"}, {\"type\": \"INVESTOR_PROFILE_INFO\", \"data\": {\"annualIncomeRange\": \"$100,000 - $199,999\", \"investmentObjectives\": \"LONG_TERM\", \"investmentExperience\": \"YRS_10_\", \"networthLiquidRange\": \"$0 - $4,999\", \"networthTotalRange\": \"$0 - $4,999\", \"riskTolerance\": \"Speculative Risk\"}, \"description\": \"Net worth, income, risk tolerance, experience, etc.\"}, {\"type\": \"PERSONAL_INFO\", \"data\": {\"birthdate\": \"1992-11-27\"}, \"description\": \"Birth date, gender, marital status, etc.\"}, {\"type\": \"TAX_INFO\", \"data\": {\"usTaxpayer\": false, \"taxTreatyWithUS\": false}, \"description\": \"Tax Information\"}], \"wlpID\": \"GAIN\", \"referralCode\": \"78AA48\", \"createdWhen\": \"2022-09-05T11:25:43.363Z\", \"updatedWhen\": \"2022-09-05T11:25:43.363Z\"}"')
on conflict do nothing;

INSERT INTO app.profile_plaid_access_tokens (profile_id, access_token, item_id, institution_id,
                                             needs_reauth_since, purpose)
select t.profile_id,
       t.access_token,
       t.item_id,
       t.purpose
from (
         select 2                                                     as profile_id,
                'access-sandbox-b7033717-380d-4a40-a0a9-acfee365eaea' as access_token,
                'lK3VVvnW7ZH9Bdnqe11RI86KLXZDNDiZA6vpy'               as item_id,
                'trading'                                             as purpose
     ) t
         left join app.profile_plaid_access_tokens using (profile_id, purpose)
where profile_plaid_access_tokens is null;

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


INSERT INTO app.trading_accounts (id, profile_id, name, cash_available_for_trade, cash_available_for_withdrawal,
                                  cash_balance)
VALUES (1, 2, 'Mikhail''s Robo Advisor Managed Account', 0, 0, 0);

INSERT INTO app.drivewealth_accounts (ref_id, drivewealth_user_id, trading_account_id, status, ref_no, nickname,
                                      cash_available_for_trade, cash_available_for_withdrawal, cash_balance, data)
VALUES ('bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557', 'bf98c335-57ad-4337-ae9f-ed1fcfb447af', 1, 'PENDING',
        'GYEK000001', 'Mikhail''s Robo Advisor Managed Account', 0, 0, 0,
        '"{\"id\": \"bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557\", \"accountNo\": \"GYEK000001\", \"accountType\": {\"name\": \"LIVE\", \"description\": \"Live Account\"}, \"accountMgmtType\": {\"name\": \"RIA_MANAGED\", \"description\": \"Robo Advisor Managed Account\"}, \"status\": {\"name\": \"PENDING\", \"description\": \"Pending\"}, \"tradingType\": {\"name\": \"CASH\", \"description\": \"Cash account\"}, \"leverage\": 1.0, \"nickname\": \"Mikhail''s Robo Advisor Managed Account\", \"parentIB\": {\"id\": \"7b746acb-0afa-42c3-9c94-1bc8c16ce7b2\", \"name\": \"Gainy\"}, \"taxProfile\": {\"taxStatusCode\": \"W-9\", \"taxRecipientCode\": \"INDIVIDUAL\"}, \"commissionID\": \"4dafc263-f73a-4972-bed0-3af9a6ee3d7d\", \"beneficiaries\": false, \"userID\": \"bf98c335-57ad-4337-ae9f-ed1fcfb447af\", \"restricted\": false, \"goodFaithViolations\": 0, \"patternDayTrades\": 0, \"freeTradeBalance\": 0, \"gfvPdtExempt\": false, \"buyingPowerOverride\": false, \"bod\": {}, \"ria\": {\"advisorID\": \"7b746acb-0afa-42c3-9c94-1bc8c16ce7b2.1661277115494\", \"productID\": \"product_e5046072-eefc-47ed-90d4-60654c33cf92\"}, \"sweepInd\": true, \"interestFree\": false, \"openedWhen\": \"2022-09-05T11:25:45Z\", \"ignoreMarketHoursForTest\": true, \"flaggedForACATS\": false}"')
on conflict do nothing;
