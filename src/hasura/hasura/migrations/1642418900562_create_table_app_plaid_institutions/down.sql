alter table "app"."profile_plaid_access_tokens"
    drop constraint "profile_plaid_access_tokens_institution_id_fkey";

alter table "app"."profile_plaid_access_tokens"
    drop column "institution_id";

DROP TABLE "app"."plaid_institutions";
