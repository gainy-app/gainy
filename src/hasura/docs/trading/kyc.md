# Trading KYC
- query to fill KYC fields (*kyc_form*)
- mutation to fill KYC fields (*kyc_form*)
- action to get KYC form placeholders (*kyc_form_config*)
- action to send KYC data to DW, store and return KYC status (*drivewealth_users, drivewealth_accounts, drivewealth_kyc_status*)
  - TradingService.sendToKyc(kyc_form)
- action to get KYC status (*drivewealth_kyc_status*)
  - TradingService.getKycStatus(profile_id)
- action to generate pre-signed-url https://docs.aws.amazon.com/AmazonS3/latest/userguide/PresignedUrlUploadObject.html (*uploaded_documents*)
- mutation to set file as uploaded (*uploaded_documents*)
- mutation to upload a document to KYC (*uploaded_documents, kyc_documents*)
- trigger to send kyc_documents to drivewealth (*drivewealth_kyc_documents*)
  - TradingService.uploadKycDocument(profile_id, kyc_document)

Data used: kyc_form, kyc_form_config, kyc_documents, drivewealth_users, drivewealth_accounts, drivewealth_kyc_status, trading_account

## Data

DW KYC documents
```python
documents = [
    {
        "type": "BASIC_INFO",
        "data.firstName": kycForm['first_name'],
        "data.lastName": kycForm['last_name'],
        "data.country": "USA",
        "data.phone": kycForm['phone_number'],
        "data.emailAddress": kycForm['email_address'],
        "data.language": "en_US"
    },
    {
        "type": "EMPLOYMENT_INFO",
        "data.status": kycForm['employment_status'],
        "data.company":	kycForm['employment_company_name'],
        "data.type": kycForm['employment_type'],
        "data.position": kycForm['employment_position'],
        "data.broker": kycForm['employment_affiliated_with_a_broker'],
        "data.directorOf": kycForm['employment_is_director_of_a_public_company']
    },
    {
        "type": "INVESTOR_PROFILE_INFO",
        "data.investmentExperience": kycForm['investor_profile_experience'],
        "data.annualIncome": kycForm['investor_profile_annual_income'],
        "data.networthTotal": kycForm['investor_profile_net_worth_total'],
        "data.riskTolerance": kycForm['investor_profile_risk_tolerance'],
        "data.investmentObjectives": kycForm['investor_profile_objectives'],
        "data.networthLiquid": kycForm['investor_profile_net_worth_liquid'],
    },
    {
        "type": "DISCLOSURES",
        "data.termsOfUse": kycForm['disclosures_drivewealth_terms_of_use'],
        "data.customerAgreement": kycForm['disclosures_drivewealth_customer_agreement'],
        "data.iraAgreement": kycForm['disclosures_drivewealth_ira_agreement'],
        "data.marketDataAgreement": kycForm['disclosures_drivewealth_market_data_agreement'],
        "data.rule14b": kycForm['disclosures_rule14b'],
        "data.findersFee": false
        "data.privacyPolicy": kycForm['disclosures_drivewealth_privacy_policy'],
        "data.dataSharing": kycForm['disclosures_drivewealth_data_sharing'],
        "data.signedBy": kycForm['disclosures_signed_by'],
        "data.extendedHoursAgreement": kycForm['disclosures_extended_hours_agreement'],
    },
    {
        "type": "IDENTIFICATION_INFO",
        "data.value": kycForm['tax_id_value'].replace('-', '') if 'tax_id_value' in kycForm and kycForm['tax_id_value'] else None,
        "data.type": kycForm['tax_id_type'],
        "data.citizenship": kycForm['citizenship'],
        "data.usTaxPayer": kycForm['is_us_tax_payer'],
    },
    {
        "type": "TAX_INFO",
        "data.taxTreatyWithUS": kycForm['tax_treaty_with_us'],
    },
    {
        "type": "PERSONAL_INFO",
        "data.birthDay": kycForm['birthdate'].day,
        "data.birthMonth": kycForm['birthdate'].month,
        "data.birthYear": kycForm['birthdate'].year,
        "data.politicallyExposedNames": kycForm['politically_exposed_names'],
        "data.irsBackupWithholdings": kycForm['irs_backup_withholdings_notified'],
        "data.gender": kycForm['gender'],
        "data.marital": kycForm['marital_status'],
    },
    {
        "type": "ADDRESS_INFO",
        "data.street1": kycForm['address_street1'],
        "data.street2": kycForm['address_street2'],
        "data.city": kycForm['address_city'],
        "data.province": kycForm['address_province'],
        "data.postalCode": kycForm['address_postal_code'],
        "data.country": kycForm['address_country'],
    },
]
```

- kyc_form
  - first_name: string
  - last_name: string
  - country: string
  - phone_number: string
  - email_address: string
  - language: string
    **[hidden in the app]**

  - employment_status: string
  - employment_company_name: string
    >Only required when `employment_status` is `EMPLOYED` or `SELF_EMPLOYED`
  - employment_type: string
    >Only required when `employment_status` is `EMPLOYED` or `SELF_EMPLOYED`
  - employment_position: string
    >Only required when `employment_status` is `EMPLOYED` or `SELF_EMPLOYED`
  - employment_affiliated_with_a_broker: boolean
    > Official question: "Are they affiliated with or employed by a stock exchange, member firm of an exchange or FINRA, or a municipal securities broker-dealer?"
    
    > If true, display: "If answered Yes, you are obligated to notify your employer in writing of your intention to open an account."
  - employment_is_director_of_a_public_company: string
    > Official question: "Is the account holder(s) a control person of a publicly traded company? A Director, Officer or 10% stock owner?"

    > if "Yes", send string and display: "If yes, please list the company name and its ticker symbol."

    > If "No", send null

  - investor_profile_experience: string
  - investor_profile_annual_income: bigint
    > In USD
  - investor_profile_net_worth_total: bigint
    > In USD
  - investor_profile_risk_tolerance: string
  - investor_profile_objectives: string
  - investor_profile_net_worth_liquid: bigint
    > In USD

  - disclosures_drivewealth_terms_of_use: boolean
    > Acceptance of DriveWealth's Terms Of Use
  - disclosures_drivewealth_customer_agreement: boolean
    > USED FOR STANDARD BROKERAGE ACCOUNTS
    
    > Acceptance of DriveWealth's Customer Agreement
  - disclosures_drivewealth_ira_agreement: boolean
    **[hidden in the app]**
    > USED FOR RETIREMENT ACCOUNTS

    > Acceptance of DriveWealth's IRA Agreement
  - disclosures_drivewealth_market_data_agreement: boolean
    > Acceptance of DriveWealth's Market Data Agreement
  - disclosures_rule14b: boolean
    > Acceptance of Rule 14b1(c).

    > Official Copy: Rule 14b-1(c) of the Securities Exchange Act, unless you object, requires us to disclose to an issuer, upon its request, the names, addresses, and securities positions of our customers who are beneficial owners of the issuer's securities held by us in nominee name. The issuer would be permitted to use your name and other related information for corporation communication only.
  - disclosures_drivewealth_privacy_policy: boolean
    > Acceptance of DriveWealth's Privacy Policy
  - disclosures_drivewealth_data_sharing: boolean
    > Acceptance of DriveWealths Data Sharing Policy, includes New Services, Promotional and Marketing Consent
  - disclosures_signed_by: string
    > Name of user creating account
  - disclosures_extended_hours_agreement: boolean
    > Extended Hours Agreement if the Partner is opting in

  - tax_id_value: string
  - tax_id_type: string
  - citizenship: string
  - is_us_tax_payer: boolean
  - tax_treaty_with_us: boolean
    **[hidden in the app since we work with US only]**
    > I certify that the beneficial owner is a resident of [COUNTRY] within the meaning of the income tax treaty between the United States and that country

  - birthdate: date
  - politically_exposed_names: string
    > Official Question: "Is the account maintained for a current or former Politically Exposed Person or Public Official (includes U.S. and Foreign)? A politically exposed person is someone who has been entrusted with a prominent public function, or who is closely related to such a person."

    > If "Yes", display the following text input prompt and send the response: "Please provide the names of that official and official's immediate family members (including former spouses)."

    > If "No", send null
  - irs_backup_withholdings_notified: boolean
    >️ Official Question: "Have you been notified by the IRS that you are subject to backup withholding?"
  - gender: string
  - marital_status: string

  - address_street1: string
  - address_street2: string
  - address_city: string
  - address_province: string
    > For U.S., use USPS postal abbreviations, outside U.S. use the standard province/territory name
  - address_postal_code: string
  - address_country: string

- kyc_form_config
```yaml
firstName:
  placeholder: profile.first_name
lastName:
  placeholder: profile.last_name
country: 
  placeholder: USA
  choices:
    alpha-3: name # from raw_data.gainy_countries
phoneNumber:
emailAddress:
  placeholder: profile.email
language:
  placeholder: en_US
  choices:
    en_US: English
    zh_CN: Chinese
    es_ES: Spanish
    pt_BR: Portuguese

employment_status:
  choices:
    EMPLOYED: Employed
    RETIRED: Retired
    STUDENT: Student
    UNEMPLOYED: Not Employed
SELF_EMPLOYED: Self Employed / Business Owner
employment_company_name:
employment_type:
  choices:
    AGRICULTURE: Agriculture, Forestry, Fishing and Hunting
    MINING: Mining, Quarrying, and Oil and Gas Extraction
    UTILITIES: Utilities
    CONSTRUCTION: Construction
    MANUFACTURING: Manufacturing
    WHOLESALE: Wholesale Trade
    RETAIL: Retail Trade
    TRANSPORT: Transportation and Warehousing
    INFORMATION: Information
    FINANCE: Finance and Insurance
    REAL_ESTATE: Real Estate and Rental and Leasing
    PROFESSIONAL: Professional, Scientific, and Technical Services
    MANAGEMENT: Management of Companies and Enterprises
    EDUCATION: Educational Services
    HEALTH: Health Care and Social Assistance
    ART: Arts, Entertainment, and Recreation
    FOOD: Accommodation and Food Services
    PUBLIC: Public Administration
    WASTE: Administrative and Support and Waste Management and Remediation Services
employment_position:
  choices:
    ACCOUNTANT: Accountant/CPA/Bookkeeper/Controller
    ACTUARY: Actuary
    ADJUSTER: Adjuster
    ADMINISTRATOR: Administrator
    ADVERTISER: Advertiser/Marketer/PR Professional
    AGENT: Agent
    ATC: Air Traffic Controller
    AMBASSADOR: Ambassador/Consulate Professional
    ANALYST: Analyst
    APPRAISER: Appraiser
    ARCHITECT: Architect/Designer
    ARTIST: Artist/Performer/Actor/Dancer
    ASSISTANT: Assistant
    ATHLETE: Athlete
    ATTENDANT: Attendant
    ATTORNEY: Attorney/Judge/Legal Professional
    AUCTIONEER: Auctioneer
    AUDITOR: Auditor
    BARBER: Barber/Beautician/Hairstylist
    BROKER: Broker
    BUSINESS_EXEC: Business Executive (VP, Director, etc.)
    BUSINESS_OWNER: Business Owner
    CAREGIVER: Caregiver
    CARPENTER: Carpenter/Construction Worker
    CASHIER: Cashier
    CHEF: Chef/Cook
    CHIROPRACTOR: Chiropractor
    CIVIL: Civil Servant
    CLERGY: Clergy
    CLERK: Clerk
    COMPLIANCE: Compliance/Regulatory Professional
    CONSULTANT: Consultant
    CONTRACTOR: Contractor
    COUNSELOR: Counselor/Therapist
    CUSTOMER_SERVICE: Customer Service Representative
    DEALER: Dealer
    DEVELOPER: Developer
    DISTRIBUTOR: Distributor
    DOCTOR: Doctor/Dentist/Veterinarian/Surgeon
    DRIVER: Driver
    ENGINEER: Engineer
    EXAMINER: Examiner
    EXTERMINATOR: Exterminator
    FACTORY: Factory/Warehouse Worker
    FARMER: Farmer/Rancher
    FINANCIAL: Financial Planner
    FISHERMAN: Fisherman
    FLIGHT: Flight Attendant
    HR: Human Resources Professional
    IMPEX: Importer/Exporter
    INSPECTOR: Inspector/Investigator
    INTERN: Intern
    INVESTMENT: Investment Advisor/Investment Manager
    INVESTOR: Investor
    IT: IT Professional/IT Associate
    JANITOR: Janitor
    JEWELER: Jeweler
    LABORER: Laborer
    LANDSCAPER: Landscaper
    LENDING: Lending Professional
    MANAGER: Manager
    MECHANIC: Mechanic
    MILITARY: Military, Officer or Associated
    MORTICIAN: Mortician/Funeral Director
    NURSE: Nurse
    NUTRITIONIST: Nutritionist
    OFFICE: Office Associate
    PHARMACIST: Pharmacist
    PHYSICAL: Physical Therapist
    PILOT: Pilot
    POLICE: Police Officer/Firefighter/Law Enforcement Professional
    POLITICIAN: Politician
    PM: Project Manager
    REP: Registered Rep
    RESEARCHER: Researcher
    SAILOR: Sailor/Seaman
    SALES: Salesperson
    SCIENTIST: Scientist
    SEAMSTRESS: Seamstress/Tailor
    SECURITY: Security Guard
    SOCIAL: Social Worker
    TEACHER: Teacher/Professor
    TECHNICIAN: Technician
    TELLER: Teller
    TRADESPERSON: Tradesperson/Craftsperson
    TRAINER: Trainer/Instructor
    TRANSPORTER: Transporter
    UNDERWRITER: Underwriter
    WRITER: Writer/Journalist/Editor
employment_affiliated_with_a_broker:
employment_is_director_of_a_public_company:

investor_profile_experience:
  choices:
    NONE: None
    YRS_1_2: 1–2 years
    YRS_3_5: 3–5 years
    YRS_5_10: 5–10 years
    YRS_10_: 10+ years
investor_profile_annual_income:
investor_profile_net_worth_total:
investor_profile_risk_tolerance:
  choices:
    LOW: Low Risk
    MODERATE: Moderate Risk
    SPECULATION: Speculative Risk
    HIGH: High Risk
investor_profile_objectives:
  choices:
    LONG_TERM: Long–term buy & hold investing
    INFREQUENT: Trading infrequently when I see an opportunity
    FREQUENT: Frequent trader, depending on the market
    ACTIVE_DAILY: Active trader, daily trader
    NEW: New to investing
investor_profile_net_worth_liquid:

disclosures_drivewealth_terms_of_use:
disclosures_drivewealth_customer_agreement:
disclosures_drivewealth_ira_agreement:
disclosures_drivewealth_market_data_agreement:
disclosures_rule14b:
disclosures_drivewealth_privacy_policy:
disclosures_drivewealth_data_sharing:
disclosures_signed_by:
disclosures_extended_hours_agreement:

tax_id_value:
tax_id_type:
  choices:
    SSN: Social Security Number
    TIN: Tax Identification Number
    other: Other
citizenship:
  placeholder: USA
  choices:
    alpha-3: name # from raw_data.gainy_countries
is_us_tax_payer:

birthdate: 
politically_exposed_names: 
irs_backup_withholdings_notified: 
gender:
  placeholder: "Male" if profile['gender'] == 1 else "Female" if profile['gender'] == 0 else None  
  required: false
  choices: 
    Male: Male
    Female: Female
marital_status:
  choices:
    SINGLE: Single
    DIVORCED: Divorced
    MARRIED: Married
    WIDOWED: Widowed
    PARTNER: Domestic Partner

address_street1:
address_street2:
  required: false 
address_city:
address_province:
address_postal_code:
address_country:
  placeholder: USA
  choices:
    alpha-3: name # from raw_data.gainy_countries
```

- uploaded_documents
  id: int
  profile_id: int
  s3_bucket: string
  s3_key: string
  is_uploaded: boolean

- kyc_documents
  type: string # `DRIVER_LICENSE | PASSPORT | NATIONAL_ID_CARD | VOTER_ID | WORK_PERMIT | VISA | RESIDENCE_PERMIT`
  side: string # `FRONT | BACK` only available for types `DRIVER_LICENSE | NATIONAL_ID_CARD | VOTER_ID | WORK_PERMIT | RESIDENCE_PERMIT`
  uploaded_document_id: int

- drivewealth_users
  - id: int
  - profile_id: int
  - ref_id: string
  - status: string

- drivewealth_accounts
  - id: int
  - drivewealth_user_id: int
  - trading_account_id: int
  - ref_id: string
  - ref_no: string
  - nickname: string
  - cash_balance: integer
  - data: json

- trading_accounts # one to one at this point?
  - id: int
  - profile_id: int
  - name: string
  - cash_balance: int

- drivewealth_kyc_documents
  - drivewealth_user_id: int
  - ref_id: string
  - kyc_document_id: int

- drivewealth_kyc_status
  - drivewealth_user_id: int
  - status: string # `KYC_NOT_READY | KYC_READY | KYC_PROCESSING | KYC_APPROVED | KYC_INFO_REQUIRED | KYC_DOC_REQUIRED | KYC_MANUAL_REVIEW | KYC_DENIED`
  - is_approved: boolean
  - is_accepted: boolean
  - errors: json
  - data: json
