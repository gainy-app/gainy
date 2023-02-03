alter table app.drivewealth_portfolio_statuses
    add column date date;

update app.drivewealth_portfolio_statuses
set date = (drivewealth_portfolio_statuses.created_at - interval '5 hours')::date;
