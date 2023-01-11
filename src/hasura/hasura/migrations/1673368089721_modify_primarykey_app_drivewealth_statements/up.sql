BEGIN TRANSACTION;
ALTER TABLE "app"."drivewealth_statements" DROP CONSTRAINT "drivewealth_statements_pkey";

ALTER TABLE "app"."drivewealth_statements"
    ADD CONSTRAINT "drivewealth_statements_pkey" PRIMARY KEY ("account_id", "type", "file_key");
COMMIT TRANSACTION;
