alter table app.notifications
    rename column onesignal_template_id to template_id;
alter table app.notifications
    rename column push_response to response;
alter table app.notifications
    drop column is_email,
    drop column email_response;
