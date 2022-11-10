{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}

with raw_data_0d as
         (
             select symbol,
                    historical_prices.date           as date_0d,
                    historical_prices.adjusted_close as price_0d
             from {{ ref('historical_prices') }}
                      join (
                               select symbol,
                                      max(historical_prices.date) as date
                               from {{ ref('historical_prices') }}
                               group by symbol
                           ) t
                           using (symbol, date)
         ),
     raw_data_1w as
         (
             select raw_data_0d.*,
                    historical_prices.date           as date_1w,
                    historical_prices.adjusted_close as price_1w
             from raw_data_0d
                      left join (
                                    select symbol,
                                           max(historical_prices.date) as date
                                    from {{ ref('historical_prices') }}
                                    where date < now()::date - interval '1 week'
                                    group by symbol
                                ) t
                                using (symbol)
                      left join {{ ref('historical_prices') }} using (symbol, date)
         ),
     raw_data_10d as
         (
             select distinct on (
                 raw_data_1w.symbol
                 ) raw_data_1w.*,
                   historical_prices.date           as date_10d,
                   historical_prices.adjusted_close as price_10d
             from raw_data_1w
                      left join {{ ref('historical_prices') }}
                                on historical_prices.symbol = raw_data_1w.symbol
                                    and date < now()::date - interval '10 days'
                                    and date > now()::date - interval '10 days' - interval '1 week'
             order by symbol, date desc
         ),
     raw_data_1m as
         (
             select distinct on (
                 raw_data_10d.symbol
                 ) raw_data_10d.*,
                   historical_prices.date           as date_1m,
                   historical_prices.adjusted_close as price_1m
             from raw_data_10d
                      left join {{ ref('historical_prices') }}
                                on historical_prices.symbol = raw_data_10d.symbol
                                    and date < now()::date - interval '1 month'
                                    and date > now()::date - interval '1 month' - interval '1 week'
             order by symbol, date desc
         ),
     raw_data_2m as
         (
             select distinct on (
                 raw_data_1m.symbol
                 ) raw_data_1m.*,
                   historical_prices.date           as date_2m,
                   historical_prices.adjusted_close as price_2m
             from raw_data_1m
                      left join {{ ref('historical_prices') }}
                                on historical_prices.symbol = raw_data_1m.symbol
                                    and date < now()::date - interval '2 month'
                                    and date > now()::date - interval '2 month' - interval '1 week'
             order by symbol, date desc
         ),
     raw_data_3m as
         (
             select distinct on (
                 raw_data_2m.symbol
                 ) raw_data_2m.*,
                   historical_prices.date           as date_3m,
                   historical_prices.adjusted_close as price_3m
             from raw_data_2m
                      left join {{ ref('historical_prices') }}
                                on historical_prices.symbol = raw_data_2m.symbol
                                    and date < now()::date - interval '3 month'
                                    and date > now()::date - interval '3 month' - interval '1 week'
             order by symbol, date desc
         ),
     raw_data_1y as
         (
             select raw_data_3m.*,
                   historical_prices.date           as date_1y,
                   historical_prices.adjusted_close as price_1y
             from raw_data_3m
                      left join {{ ref('historical_prices') }}
                                on historical_prices.symbol = raw_data_3m.symbol
                                    and date < now()::date - interval '1 year'
                                    and date > now()::date - interval '1 year' - interval '1 week'
             order by symbol, date desc
         ),
     raw_data_13m as
         (
             select distinct on (
                 raw_data_1y.symbol
                 ) raw_data_1y.*,
                   historical_prices.date           as date_13m,
                   historical_prices.adjusted_close as price_13m
             from raw_data_1y
                      left join {{ ref('historical_prices') }}
                                on historical_prices.symbol = raw_data_1y.symbol
                                    and date < now()::date - interval '13 month'
                                    and date > now()::date - interval '13 month' - interval '1 week'
             order by symbol, date desc
         ),
     raw_data_5y as
         (
             select distinct on (
                 raw_data_13m.symbol
                 ) raw_data_13m.*,
                   historical_prices.date           as date_5y,
                   historical_prices.adjusted_close as price_5y
             from raw_data_13m
                      left join {{ ref('historical_prices') }}
                                on historical_prices.symbol = raw_data_13m.symbol
                                    and date < now()::date - interval '5 year'
                                    and date > now()::date - interval '5 year' - interval '1 week'
             order by symbol, date desc
         ),
     raw_data_all as
         (
             select raw_data_5y.*,
                    historical_prices.date           as date_all,
                    historical_prices.adjusted_close as price_all
             from raw_data_5y
                      join (
                               select symbol,
                                      min(historical_prices.date) as date
                               from {{ ref('historical_prices') }}
                               group by symbol
                           ) t
                           using (symbol)
                      join {{ ref('historical_prices') }} using (symbol, date)
         )
select symbol,
       date_0d,
       price_0d,
       date_1w,
       price_1w,
       date_10d,
       price_10d,
       date_1m,
       price_1m,
       date_2m,
       price_2m,
       date_3m,
       price_3m,
       date_1y,
       price_1y,
       date_13m,
       price_13m,
       date_5y,
       price_5y,
       date_all,
       price_all
from raw_data_all
