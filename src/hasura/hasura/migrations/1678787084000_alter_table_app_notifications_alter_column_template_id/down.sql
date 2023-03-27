alter table app.notifications
    rename column onesignal_template_id to template_id;
alter table app.notifications
    rename column push_response to response;

begin;
delete from app.notifications where text is null;
alter table app.notifications
    alter column text set not null,
    drop column notification_method,
    drop column notification_params,
    drop column notification_sent;
commit;
