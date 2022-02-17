create table "app"."profile_recommendations_metadata" (
    profile_id int4 primary key,
    recommendations_version int4,
    updated_at timestamptz default now()
);
