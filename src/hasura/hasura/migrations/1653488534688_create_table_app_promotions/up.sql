CREATE TABLE "app"."subscriptions"
(
    "id"                          serial      NOT NULL,
    "profile_id"                  integer     NOT NULL,
    "invitation_id"               integer,
    "is_promotion"                bool                 DEFAULT FALSE,
    "created_at"                  timestamptz NOT NULL DEFAULT now(),
    "period"                      interval    NOT NULL,
    "revenuecat_ref_id"           integer,
    "revenuecat_entitlement_data" jsonb,
    PRIMARY KEY ("id"),
    UNIQUE ("invitation_id")
);
