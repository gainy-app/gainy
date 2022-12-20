alter table "app"."queue_messages"
    add column version int not null default 0;