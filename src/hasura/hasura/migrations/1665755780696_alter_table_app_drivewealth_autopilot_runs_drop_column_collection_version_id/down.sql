alter table "app"."drivewealth_autopilot_runs"
    add column "collection_version_id" integer not null
        references app.trading_collection_versions
            on update cascade on delete cascade;
