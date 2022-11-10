CREATE TABLE "app"."drivewealth_statements"
(
    "trading_statement_id" int         NOT NULL,
    "type"                varchar     NOT NULL,
    "display_name"        varchar     NOT NULL,
    "file_key"            varchar     NOT NULL,
    "account_id"          varchar     NOT NULL,
    "user_id"             varchar     NOT NULL,
    "created_at"          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("file_key"),
    FOREIGN KEY ("trading_statement_id") REFERENCES "app"."trading_statements" ("id") ON UPDATE set null ON DELETE set null
);
