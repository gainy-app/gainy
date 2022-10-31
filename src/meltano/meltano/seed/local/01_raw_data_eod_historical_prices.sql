insert into raw_data.eod_historical_prices (code, adjusted_close, close, date, high, low, open, volume)
with latest_historical_prices as
         (
             select distinct on (code) *
             from raw_data.eod_historical_prices
             order by code, date desc
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
