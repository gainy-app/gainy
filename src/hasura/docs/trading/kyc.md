# Trading / KYC

- [Get KYC form values](#get-kyc-form-values)
- [Fill KYC fields](#fill-kyc-fields)
- [Action to get KYC form placeholders](#action-to-get-kyc-form-placeholders)
- [Send KYC data](#send-kyc-data)
- [Get KYC status](#get-kyc-status)
- [Generate pre-signed-url to upload documents](#generate-pre-signed-url-to-upload-documents)
- [Add KYC uploaded Document](#add-kyc-uploaded-document)

### Fill KYC fields
```graphql
mutation UpsertKycForm (
  $address_city: String
  $address_country: String
  $address_postal_code: String
  $address_province: String
  $address_street1: String
  $address_street2: String
  $birthdate: date
  $citizenship: String
  $country: String
  $disclosures_drivewealth_customer_agreement: Boolean
  $disclosures_drivewealth_data_sharing: Boolean
  $disclosures_drivewealth_ira_agreement: Boolean
  $disclosures_drivewealth_market_data_agreement: Boolean
  $disclosures_drivewealth_privacy_policy: Boolean
  $disclosures_drivewealth_terms_of_use: Boolean
  $disclosures_extended_hours_agreement: Boolean
  $disclosures_rule14b: Boolean
  $disclosures_signed_by: String
  $email_address: String
  $employment_affiliated_with_a_broker: Boolean
  $employment_company_name: String
  $employment_is_director_of_a_public_company: String
  $employment_position: String
  $employment_status: String
  $employment_type: String
  $first_name: String
  $gender: String
  $investor_profile_annual_income: bigint
  $investor_profile_experience: String
  $investor_profile_net_worth_liquid: bigint
  $investor_profile_net_worth_total: bigint
  $investor_profile_objectives: String
  $investor_profile_risk_tolerance: String
  $tax_treaty_with_us: Boolean
  $tax_id_value: String
  $tax_id_type: String
  $profile_id: Int!
  $politically_exposed_names: String
  $phone_number: String
  $marital_status: String
  $last_name: String
  $language: String
  $is_us_tax_payer: Boolean
  $irs_backup_withholdings_notified: Boolean
){
  insert_app_kyc_form(on_conflict: {
    constraint: kyc_form_pkey, 
    update_columns: [
      address_city
      address_country
      address_postal_code
      address_province
      address_street1
      address_street2
      birthdate
      citizenship
      country
      disclosures_drivewealth_customer_agreement
      disclosures_drivewealth_data_sharing
      disclosures_drivewealth_ira_agreement
      disclosures_drivewealth_market_data_agreement
      disclosures_drivewealth_privacy_policy
      disclosures_drivewealth_terms_of_use
      disclosures_extended_hours_agreement
      disclosures_rule14b
      disclosures_signed_by
      email_address
      employment_affiliated_with_a_broker
      employment_company_name
      employment_is_director_of_a_public_company
      employment_position
      employment_status
      employment_type
      first_name
      gender
      investor_profile_annual_income
      investor_profile_experience
      investor_profile_net_worth_liquid
      investor_profile_net_worth_total
      investor_profile_objectives
      investor_profile_risk_tolerance
      tax_treaty_with_us
      tax_id_value
      tax_id_type
      profile_id
      politically_exposed_names
      phone_number
      marital_status
      last_name
      language
      is_us_tax_payer
      irs_backup_withholdings_notified
    ]
  }, objects: [
    {
      address_city: $address_city
      address_country: $address_country
      address_postal_code: $address_postal_code
      address_province: $address_province
      address_street1: $address_street1
      address_street2: $address_street2
      birthdate: $birthdate
      citizenship: $citizenship
      country: $country
      disclosures_drivewealth_customer_agreement: $disclosures_drivewealth_customer_agreement
      disclosures_drivewealth_data_sharing: $disclosures_drivewealth_data_sharing
      disclosures_drivewealth_ira_agreement: $disclosures_drivewealth_ira_agreement
      disclosures_drivewealth_market_data_agreement: $disclosures_drivewealth_market_data_agreement
      disclosures_drivewealth_privacy_policy: $disclosures_drivewealth_privacy_policy
      disclosures_drivewealth_terms_of_use: $disclosures_drivewealth_terms_of_use
      disclosures_extended_hours_agreement: $disclosures_extended_hours_agreement
      disclosures_rule14b: $disclosures_rule14b
      disclosures_signed_by: $disclosures_signed_by
      email_address: $email_address
      employment_affiliated_with_a_broker: $employment_affiliated_with_a_broker
      employment_company_name: $employment_company_name
      employment_is_director_of_a_public_company: $employment_is_director_of_a_public_company
      employment_position: $employment_position
      employment_status: $employment_status
      employment_type: $employment_type
      first_name: $first_name
      gender: $gender
      investor_profile_annual_income: $investor_profile_annual_income
      investor_profile_experience: $investor_profile_experience
      investor_profile_net_worth_liquid: $investor_profile_net_worth_liquid
      investor_profile_net_worth_total: $investor_profile_net_worth_total
      investor_profile_objectives: $investor_profile_objectives
      investor_profile_risk_tolerance: $investor_profile_risk_tolerance
      tax_treaty_with_us: $tax_treaty_with_us
      tax_id_value: $tax_id_value
      tax_id_type: $tax_id_type
      profile_id: $profile_id
      politically_exposed_names: $politically_exposed_names
      phone_number: $phone_number
      marital_status: $marital_status
      last_name: $last_name
      language: $language
      is_us_tax_payer: $is_us_tax_payer
      irs_backup_withholdings_notified: $irs_backup_withholdings_notified
    }
  ]) {
    returning {
      address_city
      address_country
      address_postal_code
      address_province
      address_street1
      address_street2
      birthdate
      citizenship
      country
      disclosures_drivewealth_customer_agreement
      disclosures_drivewealth_data_sharing
      disclosures_drivewealth_ira_agreement
      disclosures_drivewealth_market_data_agreement
      disclosures_drivewealth_privacy_policy
      disclosures_drivewealth_terms_of_use
      disclosures_extended_hours_agreement
      disclosures_rule14b
      disclosures_signed_by
      email_address
      employment_affiliated_with_a_broker
      employment_company_name
      employment_is_director_of_a_public_company
      employment_position
      employment_status
      employment_type
      first_name
      gender
      investor_profile_annual_income
      investor_profile_experience
      investor_profile_net_worth_liquid
      investor_profile_net_worth_total
      investor_profile_objectives
      investor_profile_risk_tolerance
      tax_treaty_with_us
      tax_id_value
      tax_id_type
      profile_id
      politically_exposed_names
      phone_number
      marital_status
      last_name
      language
      is_us_tax_payer
      irs_backup_withholdings_notified
    }
  }
}
```

Minimal information:
```json
{"profile_id": 1, "first_name": "Mikhail", "last_name": "Astashkevich", "email_address": "qurazor1@gmail.com", "phone_number": "+1234567890", "birthdate": "1992-11-27", "address_street1": "1 Wall st.", "address_city": "New York", "address_postal_code": "12345", "tax_id_value": "123456789", "tax_id_type": "SSN", "employment_status": "UNEMPLOYED", "investor_profile_annual_income": 123456, "investor_profile_objectives": "LONG_TERM", "investor_profile_experience": "YRS_10_", "investor_profile_net_worth_liquid": 123, "investor_profile_net_worth_total": 1234, "investor_profile_risk_tolerance": "SPECULATION", "disclosures_drivewealth_terms_of_use": true, "disclosures_rule14b": true, "disclosures_drivewealth_customer_agreement": true, "disclosures_drivewealth_privacy_policy": true, "disclosures_drivewealth_market_data_agreement": true, "disclosures_signed_by": "Mikhail Astashkevich", "address_province": "CA", "address_country": "USA", "country": "USA", "citizenship": "USA"}
```

### Get KYC form values
```graphql
query GetKycForm($profile_id: Int!) {
  app_kyc_form_by_pk(profile_id: $profile_id) {
    address_city
    address_country
    address_postal_code
    address_province
    address_street1
    address_street2
    birthdate
    citizenship
    country
    disclosures_drivewealth_customer_agreement
    disclosures_drivewealth_data_sharing
    disclosures_drivewealth_ira_agreement
    disclosures_drivewealth_market_data_agreement
    disclosures_drivewealth_privacy_policy
    disclosures_drivewealth_terms_of_use
    disclosures_extended_hours_agreement
    disclosures_rule14b
    disclosures_signed_by
    email_address
    employment_affiliated_with_a_broker
    employment_company_name
    employment_is_director_of_a_public_company
    employment_position
    employment_status
    employment_type
    first_name
    gender
    investor_profile_annual_income
    investor_profile_experience
    investor_profile_net_worth_liquid
    investor_profile_net_worth_total
    investor_profile_objectives
    investor_profile_risk_tolerance
    tax_treaty_with_us
    tax_id_value
    tax_id_type
    profile_id
    politically_exposed_names
    phone_number
    marital_status
    last_name
    language
    is_us_tax_payer
    irs_backup_withholdings_notified
  }
}

```

### Action to get KYC form placeholders
```graphql
query KycGetFormConfig($profile_id: Int!) {
  kyc_get_form_config(profile_id: $profile_id) {
    first_name{
      choices
      placeholder
      required
    }
    last_name{
      choices
      placeholder
      required
    }
    country{
      choices
      placeholder
      required
    }
    email_address{
      choices
      placeholder
      required
    }
    language{
      choices
      placeholder
      required
    }
    employment_status{
      choices
      placeholder
      required
    }
    employment_type{
      choices
      placeholder
      required
    }
    employment_position{
      choices
      placeholder
      required
    }
    investor_profile_experience{
      choices
      placeholder
      required
    }
    investor_profile_risk_tolerance{
      choices
      placeholder
      required
    }
    investor_profile_objectives{
      choices
      placeholder
      required
    }
    investor_profile_annual_income{
      choices
      placeholder
      required
    }
    investor_profile_net_worth_total{
      choices
      placeholder
      required
    }
    investor_profile_net_worth_liquid{
      choices
      placeholder
      required
    }
    disclosures_drivewealth_terms_of_use{
      choices
      placeholder
      required
    }
    disclosures_drivewealth_customer_agreement{
      choices
      placeholder
      required
    }
    disclosures_drivewealth_market_data_agreement{
      choices
      placeholder
      required
    }
    disclosures_rule14b{
      choices
      placeholder
      required
    }
    disclosures_drivewealth_privacy_policy{
      choices
      placeholder
      required
    }
    disclosures_signed_by{
      choices
      placeholder
      required
    }
    tax_id_value{
      choices
      placeholder
      required
    }
    tax_id_type{
      choices
      placeholder
      required
    }
    citizenship{
      choices
      placeholder
      required
    }
    gender{
      choices
      placeholder
      required
    }
    marital_status{
      choices
      placeholder
      required
    }
    birthdate{
      choices
      placeholder
      required
    }
    address_street1{
      choices
      placeholder
      required
    }
    address_city{
      choices
      placeholder
      required
    }
    address_postal_code{
      choices
      placeholder
      required
    }
    address_country{
      choices
      placeholder
      required
    }
  }
}
```

### Send KYC data
```graphql
mutation KycSendForm($profile_id: Int!) {
  kyc_send_form(profile_id: $profile_id) {
    message
    status # NOT_READY, READY, PROCESSING, APPROVED, INFO_REQUIRED, DOC_REQUIRED, MANUAL_REVIEW, DENIED
  }
}
```

### Get KYC status
```graphql
query KycGetStatus($profile_id: Int!) {
  kyc_get_status(profile_id: $profile_id) {
    message
    status # NOT_READY, READY, PROCESSING, APPROVED, INFO_REQUIRED, DOC_REQUIRED, MANUAL_REVIEW, DENIED
    updated_at
  }
}
```

### Generate pre-signed-url to upload documents
```graphql
mutation get_pre_signed_upload_form (
    $profile_id: Int!
    $upload_type: String! # must be "kyc"
    $content_type: String!
) {
  get_pre_signed_upload_form(
    profile_id: $profile_id
    upload_type: $upload_type
    content_type: $content_type
  ){
    id
    url
    method
  }
}
```

### Add KYC uploaded Document 
```graphql
mutation KycAddDocument (
  $profile_id: Int!
  $uploaded_file_id: Int!
  $type: String!  # `DRIVER_LICENSE | PASSPORT | NATIONAL_ID_CARD | VOTER_ID | WORK_PERMIT | VISA | RESIDENCE_PERMIT`
  $side: String! # `FRONT | BACK`. `PASSPORT` and `VISA` types can only have `FRONT` side
) {
  kyc_add_document(
    profile_id: $profile_id
    uploaded_file_id: $uploaded_file_id
    type: $type
    side: $side
  ){
    ok
  }
}
```

## Data

- kyc_form
  - country 
    **[can be hidden in the app]**
  - language 
    **[can be hidden in the app]**

  - employment_company_name 
    **[Only required when `employment_status` is `EMPLOYED` or `SELF_EMPLOYED`]**
  - employment_type: 
    **[Only required when `employment_status` is `EMPLOYED` or `SELF_EMPLOYED`]**
  - employment_position 
    **[Only required when `employment_status` is `EMPLOYED` or `SELF_EMPLOYED`]**
  - employment_affiliated_with_a_broker: boolean
    > Official question: "Are they affiliated with or employed by a stock exchange, member firm of an exchange or FINRA, or a municipal securities broker-dealer?"
    
    > If true, display: "If answered Yes, you are obligated to notify your employer in writing of your intention to open an account."
  - employment_is_director_of_a_public_company: string
    > Official question: "Is the account holder(s) a control person of a publicly traded company? A Director, Officer or 10% stock owner?"

    > if "Yes", send string and display: "If yes, please list the company name and its ticker symbol."

    > If "No", send null

  - investor_profile_annual_income 
    **[In USD]**
  - investor_profile_net_worth_total 
    **[In USD]**
  - investor_profile_net_worth_liquid 
    **[In USD]**

  - disclosures_drivewealth_terms_of_use 
    **[Acceptance of DriveWealth's Terms Of Use]**
  - disclosures_drivewealth_customer_agreement 
    **[Acceptance of DriveWealth's Customer Agreement]**
  - disclosures_drivewealth_ira_agreement 
    **[must be hidden in the app]** **[Acceptance of DriveWealth's IRA Agreement]**
  - disclosures_drivewealth_market_data_agreement 
    **[Acceptance of DriveWealth's Market Data Agreement]**
  - disclosures_rule14b 
    **[Acceptance of Rule 14b1(c)]**

    > Official Copy: Rule 14b-1(c) of the Securities Exchange Act, unless you object, requires us to disclose to an issuer, upon its request, the names, addresses, and securities positions of our customers who are beneficial owners of the issuer's securities held by us in nominee name. The issuer would be permitted to use your name and other related information for corporation communication only.
  - disclosures_drivewealth_privacy_policy
    **[Acceptance of DriveWealth's Privacy Policy]**
  - disclosures_drivewealth_data_sharing 
    **[can be hidden in the app]**
    **[Acceptance of DriveWealths Data Sharing Policy, includes New Services, Promotional and Marketing Consent]**
  - disclosures_signed_by
    **[Name of user creating account]**
  - disclosures_extended_hours_agreement 
    **[can be hidden in the app]** 
    **[Extended Hours Agreement if the Partner is opting in]**

  - tax_treaty_with_us: boolean
    **[hidden in the app since we work with US only]**
    > I certify that the beneficial owner is a resident of [COUNTRY] within the meaning of the income tax treaty between the United States and that country

  - politically_exposed_names: string
    > Official Question: "Is the account maintained for a current or former Politically Exposed Person or Public Official (includes U.S. and Foreign)? A politically exposed person is someone who has been entrusted with a prominent public function, or who is closely related to such a person."

    > If "Yes", display the following text input prompt and send the response: "Please provide the names of that official and official's immediate family members (including former spouses)."

    > If "No", send null
  - irs_backup_withholdings_notified: boolean
    **[can be hidden in the app]**
    >️ Official Question: "Have you been notified by the IRS that you are subject to backup withholding?"

  - address_province: string
    > For U.S., use USPS postal abbreviations, outside U.S. use the standard province/territory name


- drivewealth_kyc_status
  - ref_id: string
  - drivewealth_user_id: int
  - status: string # `KYC_NOT_READY | KYC_READY | KYC_PROCESSING | KYC_APPROVED | KYC_INFO_REQUIRED | KYC_DOC_REQUIRED | KYC_MANUAL_REVIEW | KYC_DENIED`
  - is_approved: boolean
  - is_accepted: boolean
  - errors: json
  - data: json
