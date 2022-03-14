create table if not exists raw_data.polygon_marketstatus_upcoming
(
    close    varchar,
    date     varchar not null,
    exchange varchar not null,
    name     varchar,
    open     varchar,
    status   varchar,
    constraint polygon_marketstatus_upcoming_pkey
        primary key (date, exchange)
);

insert into raw_data.polygon_marketstatus_upcoming (close, date, exchange, name, open, status)
values  (null, '2022-01-17', 'NYSE', 'Martin Luther King, Jr. Day', null, 'closed'),
        (null, '2022-01-17', 'NASDAQ', 'Martin Luther King, Jr. Day', null, 'closed'),
        (null, '2022-02-21', 'NYSE', 'Washington''s birthday', null, 'closed'),
        (null, '2022-02-21', 'NASDAQ', 'Washington''s birthday', null, 'closed'),
        (null, '2022-04-15', 'NYSE', 'Good Friday', null, 'closed'),
        (null, '2022-04-15', 'NASDAQ', 'Good Friday', null, 'closed'),
        (null, '2022-05-30', 'NYSE', 'Memorial Day', null, 'closed'),
        (null, '2022-05-30', 'NASDAQ', 'Memorial Day', null, 'closed'),
        (null, '2022-07-04', 'NYSE', 'Independence Day', null, 'closed'),
        (null, '2022-07-04', 'NASDAQ', 'Independence Day', null, 'closed'),
        (null, '2022-09-05', 'NYSE', 'Labor Day', null, 'closed'),
        (null, '2022-09-05', 'NASDAQ', 'Labor Day', null, 'closed'),
        (null, '2022-11-24', 'NYSE', 'Thanksgiving', null, 'closed'),
        (null, '2022-11-24', 'NASDAQ', 'Thanksgiving', null, 'closed'),
        ('2022-11-25T18:00:00.000Z', '2022-11-25', 'NYSE', 'Thanksgiving', '2022-11-25T14:30:00.000Z', 'early-close'),
        ('2022-11-25T18:00:00.000Z', '2022-11-25', 'NASDAQ', 'Thanksgiving', '2022-11-25T14:30:00.000Z', 'early-close'),
        (null, '2022-12-26', 'NYSE', 'Christmas', null, 'closed'),
        (null, '2022-12-26', 'NASDAQ', 'Christmas', null, 'closed')
on conflict do nothing;
