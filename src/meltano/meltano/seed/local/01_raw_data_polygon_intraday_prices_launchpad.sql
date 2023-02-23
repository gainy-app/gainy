insert into raw_data.polygon_intraday_prices_launchpad (symbol, t, o, h, l, c, v)
with latest_historical_prices as
         (
             (
                 select distinct on (contract_name) contract_name as symbol, t, o, h, l, c, v
                 from raw_data.polygon_options_historical_prices
                 order by contract_name, t desc
             )

             union all

             (
                 select distinct on (symbol) symbol, t, o, h, l, c, v
                 from raw_data.polygon_stocks_historical_prices
                 order by symbol, t desc
             )
         ),
    with_random as
        (
              select latest_historical_prices.symbol,
                     date_trunc('minute', dd::timestamp) as time,
                     random() as r1,
                     random() as r2,
                     latest_historical_prices.o,
                     latest_historical_prices.h,
                     latest_historical_prices.l,
                     latest_historical_prices.c,
                     latest_historical_prices.v
              FROM generate_series(now() - interval '1 week', now(), interval '1 minute') dd
                       join latest_historical_prices on true
              where extract(isodow from dd) < 6
     )
select symbol,
       extract(epoch from time) * 1000 as t,
       o * (r1 / 10 + 1) as o,
       h * (r1 / 10 + 1) as h,
       l * (r1 / 10 + 1) as l,
       c * (r1 / 10 + 1) as c,
       v * (r2 + 0.5) as v
FROM with_random
on conflict do nothing;
