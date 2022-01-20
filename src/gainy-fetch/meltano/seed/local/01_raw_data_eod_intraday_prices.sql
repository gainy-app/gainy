insert into raw_data.eod_intraday_prices (symbol, time, open, high, low, close, volume, granularity)
with latest_historical_prices as
         (
             select distinct on (code) *
             from raw_data.eod_historical_prices
             order by code, date desc
         )
select latest_historical_prices.code as symbol,
       dd::timestamp as time,
       latest_historical_prices.open + random() * 10 - 5 as open,
       latest_historical_prices.high + random() * 10 - 5 as high,
       latest_historical_prices.low + random() * 10 - 5 as low,
       latest_historical_prices.close + random() * 10 - 5 as close,
       latest_historical_prices.volume + random() * 10 - 5 as volume,
       60000 as granularity
FROM generate_series(now() - interval '5 day', now(), interval '1 minute') dd
         join latest_historical_prices on true
where extract(isodow from dd) < 6