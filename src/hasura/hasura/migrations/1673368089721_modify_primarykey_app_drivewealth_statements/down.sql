BEGIN TRANSACTION;
alter table "app"."drivewealth_statements" drop constraint "drivewealth_statements_pkey";
alter table "app"."drivewealth_statements"
    add constraint "drivewealth_statements_pkey"
    primary key ("file_key");
COMMIT TRANSACTION;
