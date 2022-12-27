create table if not exists raw_data.polygon_stock_splits
(
    _sdc_batched_at   timestamp,
    _sdc_deleted_at   varchar,
    _sdc_extracted_at timestamp,
    execution_date    varchar not null,
    split_from        double precision,
    split_to          double precision,
    ticker            varchar not null,
    primary key (execution_date, ticker)
);

insert into raw_data.polygon_stock_splits (_sdc_batched_at, _sdc_deleted_at, _sdc_extracted_at, execution_date, split_from, split_to, ticker)
values  ('2022-06-30 15:04:52.958174', null, '2022-06-30 15:04:52.955374', '2022-08-01', 1, 3, 'REX'),
        ('2022-06-30 15:04:52.958634', null, '2022-06-30 15:04:52.956600', '2022-07-18', 1, 20, 'GOOGL'),
        ('2022-06-30 15:04:52.958816', null, '2022-06-30 15:04:52.957288', '2022-07-18', 1, 20, 'GOOG'),
        ('2022-06-30 15:04:52.959004', null, '2022-06-30 15:04:52.957689', '2022-07-07', 10, 1, 'ITP'),
        ('2022-06-30 15:04:52.959208', null, '2022-06-30 15:04:52.958029', '2022-07-06', 50, 1, 'SOS'),
        ('2022-06-30 15:04:52.959362', null, '2022-06-30 15:04:52.958367', '2022-07-01', 1, 2, 'SAL'),
        ('2022-06-30 15:04:52.959542', null, '2022-06-30 15:04:52.958724', '2022-07-01', 1, 3, 'CTO'),
        ('2022-06-30 15:04:52.959732', null, '2022-06-30 15:04:52.959046', '2022-06-30', 100, 105, 'SNFCA'),
        ('2022-06-30 15:04:52.960099', null, '2022-06-30 15:04:52.959362', '2022-06-29', 4, 1, 'UTSI'),
        ('2022-06-30 15:04:52.960309', null, '2022-06-30 15:04:52.959692', '2022-06-29', 1, 10, 'SHOP')
on conflict do nothing;
