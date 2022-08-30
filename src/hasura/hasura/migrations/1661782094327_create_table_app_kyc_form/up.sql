CREATE TABLE "app"."kyc_form"
(
    "first_name"                                    varchar,
    "last_name"                                     varchar,
    "country"                                       varchar              default 'USA',
    "phone_number"                                  varchar,
    "email_address"                                 varchar,
    "language"                                      varchar              default 'en_US',
    "employment_status"                             varchar
        constraint employment_status
            check (employment_status is null or
                   employment_status in ('EMPLOYED', 'RETIRED', 'STUDENT', 'UNEMPLOYED', 'SELF_EMPLOYED')),
    "employment_company_name"                       varchar
        constraint employment_company_name
            check (employment_status not in ('EMPLOYED', 'SELF_EMPLOYED') or employment_company_name is not null),
    "employment_type"                               varchar
        constraint employment_type
            check (employment_status not in ('EMPLOYED', 'SELF_EMPLOYED') or employment_type is not null),
    "employment_position"                           varchar
        constraint employment_position
            check (employment_status not in ('EMPLOYED', 'SELF_EMPLOYED') or employment_position is not null),
    "employment_affiliated_with_a_broker"           boolean,
    "employment_is_director_of_a_public_company"    varchar,
    "investor_profile_experience"                   varchar
        constraint investor_profile_experience
            check (investor_profile_experience not in ('NONE', 'YRS_1_2', 'YRS_3_5', 'YRS_5_10', 'YRS_10_') or
                   investor_profile_experience is null),
    "investor_profile_annual_income"                bigint,
    "investor_profile_net_worth_total"              bigint,
    "investor_profile_risk_tolerance"               varchar
        constraint investor_profile_risk_tolerance
            check (investor_profile_risk_tolerance not in ('LOW', 'MODERATE', 'SPECULATION', 'HIGH') or
                   investor_profile_risk_tolerance is null),
    "investor_profile_objectives"                   varchar
        constraint investor_profile_objectives
            check (investor_profile_objectives not in ('LONG_TERM', 'INFREQUENT', 'FREQUENT', 'ACTIVE_DAILY', 'NEW') or
                   investor_profile_objectives is null),
    "investor_profile_net_worth_liquid"             bigint,
    "disclosures_drivewealth_terms_of_use"          boolean,
    "disclosures_drivewealth_customer_agreement"    boolean,
    "disclosures_drivewealth_ira_agreement"         boolean,
    "disclosures_drivewealth_market_data_agreement" boolean,
    "disclosures_rule14b"                           boolean,
    "disclosures_drivewealth_privacy_policy"        boolean,
    "disclosures_drivewealth_data_sharing"          boolean,
    "disclosures_signed_by"                         varchar,
    "disclosures_extended_hours_agreement"          boolean,
    "tax_id_value"                                  varchar,
    "tax_id_type"                                   varchar
        constraint tax_id_type
            check (tax_id_type not in ('SSN', 'TIN', 'other') or tax_id_type is null),
    "citizenship"                                   varchar              default 'USA',
    "is_us_tax_payer"                               boolean,
    "tax_treaty_with_us"                            boolean,
    "birthdate"                                     date,
    "politically_exposed_names"                     varchar,
    "irs_backup_withholdings_notified"              boolean,
    "gender"                                        varchar
        constraint gender
            check (gender not in ('Male', 'Female') or gender is null),
    "marital_status"                                varchar
        constraint marital_status
            check (marital_status not in ('SINGLE', 'DIVORCED', 'MARRIED', 'WIDOWED', 'PARTNER') or
                   marital_status is null),
    "address_street1"                               varchar,
    "address_street2"                               varchar,
    "address_city"                                  varchar,
    "address_province"                              varchar,
    "address_postal_code"                           varchar,
    "address_country"                               varchar,

    "status"                                        varchar
        constraint status
            check (status is null or status in ('PENDING', 'APPROVED', 'REVOKED', 'CLOSED')),
    "profile_id"                                    integer     NOT NULL,
    "created_at"                                    timestamptz NOT NULL DEFAULT now(),
    "updated_at"                                    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("profile_id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
);

CREATE OR REPLACE FUNCTION "app"."set_current_timestamp_updated_at"()
    RETURNS TRIGGER AS
$$
DECLARE
    _new record;
BEGIN
    _new := NEW;
    _new."updated_at" = NOW();
    RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_app_kyc_form_updated_at"
    BEFORE UPDATE
    ON "app"."kyc_form"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_kyc_form_updated_at" ON "app"."kyc_form"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
