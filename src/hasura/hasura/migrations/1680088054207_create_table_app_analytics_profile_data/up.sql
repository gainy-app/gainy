CREATE TABLE "app"."analytics_profile_data"
(
    "profile_id"   integer     NOT NULL,
    "service_name" text        NOT NULL,
    "metadata"     jsonb       NOT NULL,
    "created_at"   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("profile_id", "service_name"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    CONSTRAINT "service_name" CHECK (service_name in ('APPSFLYER', 'FIREBASE')),
    CONSTRAINT "metadata_appsflyer" CHECK (not (service_name = 'APPSFLYER' and (metadata -> 'appsflyer_id') is null)),
    CONSTRAINT "metadata_firebase" CHECK (not (service_name = 'FIREBASE' and (metadata -> 'app_instance_id') is null))
);
COMMENT ON TABLE "app"."analytics_profile_data" IS E'Analytics data from the client apps';
