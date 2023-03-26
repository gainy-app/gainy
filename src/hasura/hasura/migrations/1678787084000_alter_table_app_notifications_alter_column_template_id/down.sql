alter table app.notifications
    rename column onesignal_template_id to template_id;
alter table app.notifications
    rename column push_response to response;
alter table app.notifications
    drop column notification_method,
    drop column notification_params,
    drop column notification_sent;
