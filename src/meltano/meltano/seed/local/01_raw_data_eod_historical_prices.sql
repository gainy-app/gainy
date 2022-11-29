insert into raw_data.eod_historical_prices (code, adjusted_close, close, date, high, low, open, volume)
with latest_historical_prices as
         (
             select distinct on (code) *
             from raw_data.eod_historical_prices
             order by code, date desc
         ),
     exchange_schedule as
         (
             with exchanges as
                      (select *
                       from raw_data.exchanges
                       where _sdc_extracted_at >
                             (select max(_sdc_extracted_at) from raw_data.exchanges) - interval '1 minute'),
                  schedule as
                      (
                          SELECT CONCAT(dd::date, '_', exchanges.name)::varchar as id,
                                 dd::date                                       as date,
                                 exchanges.name::varchar                        as exchange_name,
                                 exchanges.country_name::varchar                as country_name,
                                 coalesce(
                                         polygon_marketstatus_upcoming.open::timestamptz,
                                         (dd::date + exchanges.open_at::time) at time zone exchanges.timezone
                                     )                                          as open_at,
                                 coalesce(
                                         polygon_marketstatus_upcoming.close::timestamptz,
                                         (dd::date + exchanges.close_at::time) at time zone exchanges.timezone
                                     )                                          as close_at
                          FROM generate_series(now() - interval '1 year 1 week', now() + interval '1 week', interval '1 day') dd
                                   join exchanges on true
                                   left join raw_data.polygon_marketstatus_upcoming
                                             ON polygon_marketstatus_upcoming.exchange = exchanges.name
                                                 and polygon_marketstatus_upcoming.date::date = dd::date
                          where extract (isodow from dd) < 6
                            and (polygon_marketstatus_upcoming.status is null
                             or polygon_marketstatus_upcoming.status != 'closed')
                      )
             select distinct on (
                 date, country_name
                 ) (date || '_' || country_name)::varchar as id,
                   date,
                   null::varchar                          as exchange_name,
                   country_name,
                   open_at,
                   close_at
             from schedule
         ),
     with_random as
         (
              select latest_historical_prices.code,
                     random() as r1,
                     random() as r2,
                     latest_historical_prices.adjusted_close,
                     latest_historical_prices.close,
                     dd::date as date,
                     latest_historical_prices.high,
                     latest_historical_prices.low,
                     latest_historical_prices.open,
                     latest_historical_prices.volume
              FROM generate_series((select min(date) from latest_historical_prices)::date + interval '1 day', now() - interval '1 day', interval '1 day') dd
                       join latest_historical_prices on true
                       join exchange_schedule
                            on exchange_schedule.country_name = 'USA'
                                and exchange_schedule.date = dd::date
              where extract(isodow from dd) < 6
     )
select code,
       adjusted_close * (r1 / 10 + 1) as adjusted_close,
       close * (r1 / 10 + 1) as close,
       date,
       high * (r1 / 10 + 1) as high,
       low * (r1 / 10 + 1) as low,
       open * (r1 / 10 + 1) as open,
       volume * (r2 + 0.5) as volume
FROM with_random
on conflict do nothing;

with min_dates as
    (
        select code, min(date) as date
        from raw_data.eod_historical_prices
        group by code
    )
update raw_data.eod_historical_prices
set first_date = min_dates.date
from min_dates
where eod_historical_prices.code = min_dates.code;
