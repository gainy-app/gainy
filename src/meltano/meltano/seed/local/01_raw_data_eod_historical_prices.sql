insert into raw_data.eod_historical_prices (code, adjusted_close, close, date, high, low, open, volume)
with latest_historical_prices as
         (
             select distinct on (code) *
             from raw_data.eod_historical_prices
             order by code, date desc
         )
select latest_historical_prices.code as code,
       latest_historical_prices.adjusted_close + random() * 10 - 5 as adjusted_close,
       latest_historical_prices.close + random() * 10 - 5 as close,
       dd::date as date,
       latest_historical_prices.high + random() * 10 - 5 as high,
       latest_historical_prices.low + random() * 10 - 5 as low,
       latest_historical_prices.open + random() * 10 - 5 as open,
       latest_historical_prices.volume * (random() + 0.5) as volume
FROM generate_series((select min(date) from latest_historical_prices)::date + interval '1 day', now() - interval '1 day', interval '1 day') dd
         join latest_historical_prices on true
where extract(isodow from dd) < 6
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
