insert into raw_data.polygon_options_historical_prices (contract_name, t, o, h, l, c, v)
with latest_historical_prices as
         (
             select distinct on (contract_name) contract_name, t, o, h, l, c, v
             from raw_data.polygon_options_historical_prices
             order by contract_name, t desc
         ),
     with_random as
         (
             select latest_historical_prices.contract_name,
                    random() as r1,
                    random() as r2,
                    dd       as t,
                    latest_historical_prices.o,
                    latest_historical_prices.h,
                    latest_historical_prices.l,
                    latest_historical_prices.c,
                    latest_historical_prices.v
             FROM generate_series((
                                      select min(t)
                                      from latest_historical_prices
                                  ) + 86400 * 1000, extract(epoch from now() - interval '1 day')::numeric * 1000,
                                  86400 * 1000) dd
                      join latest_historical_prices on true
                      join (select random() as r) r on true
             where extract(isodow from to_timestamp(dd / 1000)) < 6
     )
select contract_name,
       t,
       o * (r1 / 10 + 1),
       h * (r1 / 10 + 1),
       l * (r1 / 10 + 1),
       c * (r1 / 10 + 1),
       v * (r2 + 0.5)
FROM with_random
on conflict do nothing;
