insert into raw_data.eod_intraday_prices (symbol, time, open, high, low, close, volume, granularity)
with exchanges as
         (
             select *
             from raw_data.exchanges
             where _sdc_extracted_at >
                   (
                       select max(_sdc_extracted_at)
                       from raw_data.exchanges
                   ) - interval '1 minute'
         ),
     exchange_schedule as
         (
             SELECT dd::date                                                              as date,
                    exchanges.name::varchar                                               as exchange_name,
                    exchanges.country_name::varchar                                       as country_name,
                    (dd::date + exchanges.open_at::time) at time zone exchanges.timezone  as open_at,
                    (dd::date + exchanges.close_at::time) at time zone exchanges.timezone as close_at
             FROM generate_series(now() - interval '2 week', now(), interval '1 day') dd
                      join exchanges on true
             where extract(isodow from dd) < 6
     ),
     symbol_exchange as
         (
             select code,
                    case
                        when general ->> 'Exchange' != 'INDX'
                            then (string_to_array(general ->> 'Exchange', ' '))[1]::varchar
                        end as exchange_name
             from raw_data.eod_fundamentals
     ),
     latest_historical_prices as
         (
             select symbol as code, to_timestamp(t / 1000)::date as date, o, h, l, c, v
             from raw_data.polygon_stocks_historical_prices
             where to_timestamp(t / 1000)::date > now() - interval '2 weeks'
     ),
     with_random as
         (
             select latest_historical_prices.code,
                    dd::timestamp                                as time,
                    random()                                     as r1,
                    latest_historical_prices.o                   as open,
                    latest_historical_prices.c                   as adjusted_close,
                    latest_historical_prices.v                   as volume,
                    rank() over wnd                              as rev_rank,
                    60. / extract(epoch from close_at - open_at) as pct
             from latest_historical_prices
                      join symbol_exchange using (code)
                      join exchange_schedule using (exchange_name, date)
                      join generate_series(open_at, close_at - interval '1 minute', interval '1 minute') dd on true
                 window wnd as (partition by code, latest_historical_prices.date order by dd desc)

             union all

             select t.code,
                    dd::timestamp                                as time,
                    random()                                     as r1,
                    t.o                                          as open,
                    t.c                                          as adjusted_close,
                    t.v                                          as volume,
                    rank() over wnd                              as rev_rank,
                    60. / extract(epoch from close_at - open_at) as pct
             from (
                      select distinct on (code) * from latest_historical_prices order by code, date desc
                  ) t
                      join symbol_exchange using (code)
                      join exchange_schedule using (exchange_name)
                      join generate_series(open_at, least(close_at, now()) - interval '1 minute', interval '1 minute') dd on true
             where exchange_schedule.date > t.date
                 window wnd as (partition by code, t.date order by dd desc)
     )
select code,
       time,
       open * (r1 / 10 + 1) as open,
       open * (r1 / 10 + 1) as high,
       open * (r1 / 10 + 1) as low,
       case
           when rev_rank = 1
               then adjusted_close
           else open * (r1 / 10 + 1)
           end              as close,
       volume * pct         as volume,
       60000                as granularity
FROM with_random
on conflict do nothing;

insert into raw_data.polygon_intraday_prices (symbol, time, open, high, low, close, volume, granularity)
with exchanges as
         (
             select *
             from raw_data.exchanges
             where _sdc_extracted_at >
                   (
                       select max(_sdc_extracted_at)
                       from raw_data.exchanges
                   ) - interval '1 minute'
         ),
     exchange_schedule as
         (
             SELECT dd::date                                                              as date,
                    exchanges.name::varchar                                               as exchange_name,
                    exchanges.country_name::varchar                                       as country_name,
                    (dd::date + exchanges.open_at::time) at time zone exchanges.timezone  as open_at,
                    (dd::date + exchanges.close_at::time) at time zone exchanges.timezone as close_at
             FROM generate_series(now() - interval '2 week', now(), interval '1 day') dd
                      join exchanges on true
             where extract(isodow from dd) < 6
     ),
     symbol_exchange as
         (
             select code,
                    case
                        when general ->> 'Exchange' != 'INDX'
                            then (string_to_array(general ->> 'Exchange', ' '))[1]::varchar
                        end as exchange_name
             from raw_data.eod_fundamentals
     ),
     latest_historical_prices as
         (
             select contract_name, regexp_replace(contract_name, '^([A-Za-z]*).*$', '\1') as code, to_timestamp(t / 1000)::date as date, o, h, l, c, v
             from raw_data.polygon_options_historical_prices
             where to_timestamp(t / 1000)::date > now() - interval '2 weeks'
     ),
     with_random as
         (
             select latest_historical_prices.contract_name       as code,
                    dd::timestamp                                as time,
                    random()                                     as r1,
                    latest_historical_prices.o                   as open,
                    latest_historical_prices.c                   as adjusted_close,
                    latest_historical_prices.v                   as volume,
                    rank() over wnd                              as rev_rank,
                    60. / extract(epoch from close_at - open_at) as pct
             from latest_historical_prices
                      join symbol_exchange using (code)
                      join exchange_schedule using (exchange_name, date)
                      join generate_series(open_at, close_at - interval '1 minute', interval '1 minute') dd on true
                 window wnd as (partition by code, latest_historical_prices.date order by dd desc)

             union all

             select t.contract_name                              as code,
                    dd::timestamp                                as time,
                    random()                                     as r1,
                    t.o                                          as open,
                    t.c                                          as adjusted_close,
                    t.v                                          as volume,
                    rank() over wnd                              as rev_rank,
                    60. / extract(epoch from close_at - open_at) as pct
             from (
                      select distinct on (contract_name) * from latest_historical_prices order by contract_name, date desc
                  ) t
                      join symbol_exchange using (code)
                      join exchange_schedule using (exchange_name)
                      join generate_series(open_at, least(close_at, now()) - interval '1 minute', interval '1 minute') dd on true
             where exchange_schedule.date > t.date
                 window wnd as (partition by code, t.date order by dd desc)
     )
select code,
       time,
       open * (r1 / 10 + 1) as open,
       open * (r1 / 10 + 1) as high,
       open * (r1 / 10 + 1) as low,
       case
           when rev_rank = 1
               then adjusted_close
           else open * (r1 / 10 + 1)
           end              as close,
       volume * pct         as volume,
       60000                as granularity
FROM with_random
on conflict do nothing;
