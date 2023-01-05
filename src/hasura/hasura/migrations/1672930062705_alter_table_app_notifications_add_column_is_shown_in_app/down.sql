alter table "app"."notifications"
    drop column "is_shown_in_app",
    drop column "is_push",
    alter column "uniq_id" set not null;
