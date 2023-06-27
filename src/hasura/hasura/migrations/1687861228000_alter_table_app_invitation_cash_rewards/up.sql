alter table "app"."trading_money_flow"
    add column "type" text;

update app.trading_money_flow
set type = 'MANUAL'
where type is null;

alter table "app"."trading_money_flow"
    alter column "type" set not null;

create table app.invitation_cash_rewards
(
    invitation_id int not null,
    profile_id    int not null,
    money_flow_id int not null,
    created_at    timestamptz default now(),

    primary key (invitation_id, profile_id),
    FOREIGN KEY ("invitation_id") REFERENCES "app"."invitations" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("money_flow_id") REFERENCES "app"."trading_money_flow" ("id") ON UPDATE cascade ON DELETE cascade
);
