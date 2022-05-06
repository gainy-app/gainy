insert into raw_data.eod_historical_prices (code, adjusted_close, close, date, high, low, open, volume)
with latest_historical_prices as
         (
             select distinct on (contract_name) contract_name                as code,
                                                to_timestamp(t / 1000)::date as date,
                                                o                            as open,
                                                h                            as high,
                                                l                            as low,
                                                c                            as close,
                                                v                            as volume
             from raw_data.polygon_options_historical_prices
             order by contract_name, t desc
         )
select latest_historical_prices.code                      as code,
       latest_historical_prices.close + random() * 10 - 5 as adjusted_close,
       latest_historical_prices.close + random() * 10 - 5 as close,
       dd::date                                           as date,
       latest_historical_prices.high + random() * 10 - 5  as high,
       latest_historical_prices.low + random() * 10 - 5   as low,
       latest_historical_prices.open + random() * 10 - 5  as open,
       latest_historical_prices.volume * (random() + 0.5) as volume
FROM generate_series((
                         select min(date)
                         from latest_historical_prices
                     )::date + interval '1 day', now() - interval '1 day', interval '1 day') dd
         join latest_historical_prices on true
where extract(isodow from dd) < 6
on conflict do nothing;
