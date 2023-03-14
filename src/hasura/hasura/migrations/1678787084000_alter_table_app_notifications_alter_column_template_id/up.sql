alter table app.notifications
    rename column template_id to onesignal_template_id;
alter table app.notifications
    rename column response to push_response;
alter table app.notifications
    add column is_email bool not null default false,
    add column email_response json;
