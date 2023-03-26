alter table app.notifications
    rename column template_id to onesignal_template_id;
alter table app.notifications
    rename column response to push_response;
alter table app.notifications
    add column notification_method text,
    add column notification_params json,
    add column notification_sent bool not null default false;
