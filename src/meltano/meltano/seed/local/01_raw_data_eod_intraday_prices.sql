insert into raw_data.eod_intraday_prices (symbol, time, open, high, low, close, volume, granularity)
with latest_historical_prices as
         (
             select distinct on (code) code, open, high, low, adjusted_close, volume
             from raw_data.eod_historical_prices
             order by code, date desc
         ),
    with_random as
        (
              select latest_historical_prices.code,
                     dd::timestamp as time,
                     random() as r1,
                     random() as r2,
                     latest_historical_prices.open,
                     latest_historical_prices.high,
                     latest_historical_prices.low,
                     latest_historical_prices.adjusted_close,
                     latest_historical_prices.volume
              FROM generate_series(now() - interval '1 week', now(), interval '1 minute') dd
                       join latest_historical_prices on true
              where extract(isodow from dd) < 6
     )
select code,
       time,
       open * (r1 / 10 + 1) as open,
       high * (r1 / 10 + 1) as high,
       low * (r1 / 10 + 1) as low,
       adjusted_close * (r1 / 10 + 1) as close,
       volume * (r2 + 0.5) as volume,
       60000 as granularity
FROM with_random
on conflict do nothing;
