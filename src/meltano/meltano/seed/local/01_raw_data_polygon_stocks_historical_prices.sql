insert into raw_data.polygon_stocks_historical_prices (symbol, t, o, h, l, c, v)
with latest_historical_prices as
         (
             select distinct on (symbol) symbol, t, o, h, l, c, v
             from raw_data.polygon_stocks_historical_prices
             order by symbol, t desc
         ),
     with_random as
         (
             with exchanges as
                      (select *
                       from raw_data.exchanges
                       where _sdc_extracted_at >
                             (select max(_sdc_extracted_at) from raw_data.exchanges) - interval '1 minute')
             select latest_historical_prices.symbol,
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
                      join exchanges on true
                      left join raw_data.polygon_marketstatus_upcoming
                                ON polygon_marketstatus_upcoming.exchange = exchanges.name
                                    and polygon_marketstatus_upcoming.date::date = to_timestamp(dd / 1000)::date
                      join latest_historical_prices on true
                      join (select random() as r) r on true
             where extract(isodow from to_timestamp(dd / 1000)) < 6
               and dd> latest_historical_prices.t
               and (polygon_marketstatus_upcoming.status is null
                or polygon_marketstatus_upcoming.status != 'closed')
     )
select symbol,
       t,
       o * (r1 / 10 + 1),
       h * (r1 / 10 + 1),
       l * (r1 / 10 + 1),
       c * (r1 / 10 + 1),
       v * (r2 + 0.5)
FROM with_random
on conflict do nothing;
