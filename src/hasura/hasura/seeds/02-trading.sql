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
         select 2                                                     as profile_id,
                'access-sandbox-b7033717-380d-4a40-a0a9-acfee365eaea' as access_token,
                'lK3VVvnW7ZH9Bdnqe11RI86KLXZDNDiZA6vpy'               as item_id,
                'trading'                                             as purpose
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
VALUES (1, 2, 'Mikhail''s Robo Advisor Managed Account', 0, 0, 0);

insert into app.drivewealth_accounts (ref_id, drivewealth_user_id, trading_account_id, status, ref_no, nickname,
                                      cash_available_for_trade, cash_available_for_withdrawal, cash_balance, data)
values ('bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557', 'bf98c335-57ad-4337-ae9f-ed1fcfb447af', 1, 'OPEN',
        'GYEK000001', 'Mikhail''s Robo Advisor Managed Account', 0, 0, 0,
        '"{\"id\": \"bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557\", \"accountNo\": \"GYEK000001\", \"accountType\": {\"name\": \"LIVE\", \"description\": \"Live Account\"}, \"accountMgmtType\": {\"name\": \"RIA_MANAGED\", \"description\": \"Robo Advisor Managed Account\"}, \"status\": {\"name\": \"OPEN\", \"description\": \"Open\"}, \"tradingType\": {\"name\": \"CASH\", \"description\": \"Cash account\"}, \"leverage\": 1, \"nickname\": \"Mikhail''s Robo Advisor Managed Account\", \"parentIB\": {\"id\": \"7b746acb-0afa-42c3-9c94-1bc8c16ce7b2\", \"name\": \"Gainy\"}, \"taxProfile\": {\"taxStatusCode\": \"W-9\", \"taxRecipientCode\": \"INDIVIDUAL\"}, \"commissionID\": \"4dafc263-f73a-4972-bed0-3af9a6ee3d7d\", \"beneficiaries\": false, \"userID\": \"bf98c335-57ad-4337-ae9f-ed1fcfb447af\", \"restricted\": false, \"goodFaithViolations\": 0, \"patternDayTrades\": 0, \"freeTradeBalance\": 0, \"gfvPdtExempt\": false, \"buyingPowerOverride\": false, \"bod\": {\"moneyMarket\": 0, \"equityValue\": 0, \"cashAvailableForWithdrawal\": 0, \"cashAvailableForTrading\": 0, \"cashBalance\": 0}, \"ria\": {\"advisorID\": \"7b746acb-0afa-42c3-9c94-1bc8c16ce7b2.1661277115494\", \"productID\": \"product_e5046072-eefc-47ed-90d4-60654c33cf92\"}, \"sweepInd\": true, \"interestFree\": false, \"createdWhen\": \"2022-09-05T11:25:45.557Z\", \"openedWhen\": \"2022-09-13T05:29:45.689Z\", \"updatedWhen\": \"2022-09-13T05:29:45.689Z\", \"ignoreMarketHoursForTest\": true, \"flaggedForACATS\": false, \"extendedHoursEnrolled\": false}"')
on conflict do nothing;
