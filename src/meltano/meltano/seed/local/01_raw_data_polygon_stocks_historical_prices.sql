insert into raw_data.polygon_stocks_historical_prices (symbol, t, o, h, l, c, v)
with latest_historical_prices as
         (
             select distinct on (symbol) symbol, t, o, h, l, c, v
             from raw_data.polygon_stocks_historical_prices
             order by symbol, t desc
         )
select latest_historical_prices.symbol,
       dd                                             as t,
       latest_historical_prices.o + random() * 10 - 5 as o,
       latest_historical_prices.h + random() * 10 - 5 as h,
       latest_historical_prices.l + random() * 10 - 5 as l,
       latest_historical_prices.c + random() * 10 - 5 as c,
       latest_historical_prices.v * (random() + 0.5)  as v
FROM generate_series((
                         select min(t)
                         from latest_historical_prices
                     ) + 86400 * 1000, extract(epoch from now() - interval '1 day')::numeric * 1000, 86400 * 1000) dd
         join latest_historical_prices on true
where extract(isodow from to_timestamp(dd / 1000)) < 6;
