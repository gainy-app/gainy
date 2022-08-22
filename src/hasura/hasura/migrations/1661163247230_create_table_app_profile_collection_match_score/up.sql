CREATE TABLE "app"."profile_collection_match_score"
(
    "profile_id"          integer   NOT NULL,
    "collection_uniq_id"  text      NOT NULL,
    "match_score"         float8    NOT NULL,
    "risk_similarity"     float8    NOT NULL,
    "category_similarity" float8    NOT NULL,
    "interest_similarity" float8    NOT NULL,
    "updated_at"          timestamp NOT NULL,
    "risk_level"          integer   NOT NULL,
    "category_level"      integer   NOT NULL,
    "interest_level"      integer   NOT NULL,
    PRIMARY KEY ("profile_id", "collection_uniq_id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
);
