alter table "app"."notifications"
    add column "is_shown_in_app" boolean not null default 'false',
    add column "is_push"         boolean not null default 'false',
    alter column "uniq_id" drop not null;
