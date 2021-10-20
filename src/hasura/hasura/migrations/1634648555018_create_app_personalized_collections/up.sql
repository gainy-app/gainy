-- Tickers in personalized collections
CREATE TABLE IF NOT EXISTS "app"."personalized_ticker_collections" ("profile_id" integer NOT NULL, "collection_id" integer NOT NULL, "symbol" varchar NOT NULL, PRIMARY KEY ("profile_id","collection_id","symbol") , FOREIGN KEY ("profile_id") REFERENCES "app"."profiles"("id") ON UPDATE cascade ON DELETE cascade);
CREATE INDEX IF NOT EXISTS "app__personalized_ticker_collections__index_on__profile_id__collection_id" ON "app"."personalized_ticker_collections" ("profile_id", "collection_id");

-- Pre-calculated collection sizes for personalized collections
CREATE TABLE IF NOT EXISTS "app"."personalized_collection_sizes" ("profile_id" integer NOT NULL, "collection_id" integer NOT NULL, "size" integer NOT NULL, PRIMARY KEY ("profile_id","collection_id") , FOREIGN KEY ("profile_id") REFERENCES "app"."profiles"("id") ON UPDATE cascade ON DELETE cascade);