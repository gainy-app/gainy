{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

with raw_data_1d as
         (
             select distinct on (
                 code
                 ) code,
                   historical_prices.date           as date_0d,
                   historical_prices.adjusted_close as price_0d
             from {{ ref('historical_prices') }}
             where date > now()::date - interval '1 week'
             order by code, date desc
         ),
     raw_data_1w as
         (
             select distinct on (
                 code
                 ) raw_data_1d.*,
                   historical_prices.date           as date_1w,
                   historical_prices.adjusted_close as price_1w
             from raw_data_1d
                      join {{ ref('historical_prices') }} using (code)
             where date < now()::date - interval '1 week'
               and date > now()::date - interval '2 week'
             order by code, date desc
         ),
     raw_data_10d as
         (
             select distinct on (
                 code
                 ) raw_data_1w.*,
                   historical_prices.date           as date_10d,
                   historical_prices.adjusted_close as price_10d
             from raw_data_1w
                      join {{ ref('historical_prices') }} using (code)
             where date < now()::date - interval '10 days'
               and date > now()::date - interval '10 days' - interval '1 week'
             order by code, date desc
         ),
     raw_data_1m as
         (
             select distinct on (
                 code
                 ) raw_data_10d.*,
                   historical_prices.date           as date_1m,
                   historical_prices.adjusted_close as price_1m
             from raw_data_10d
                      join {{ ref('historical_prices') }} using (code)
             where date < now()::date - interval '1 month'
               and date > now()::date - interval '1 month' - interval '1 week'
             order by code, date desc
         ),
     raw_data_2m as
         (
             select distinct on (
                 code
                 ) raw_data_1m.*,
                   historical_prices.date           as date_2m,
                   historical_prices.adjusted_close as price_2m
             from raw_data_1m
                      join {{ ref('historical_prices') }} using (code)
             where date < now()::date - interval '2 month'
               and date > now()::date - interval '2 month' - interval '1 week'
             order by code, date desc
         ),
     raw_data_3m as
         (
             select distinct on (
                 code
                 ) raw_data_2m.*,
                   historical_prices.date           as date_3m,
                   historical_prices.adjusted_close as price_3m
             from raw_data_2m
                      join {{ ref('historical_prices') }} using (code)
             where date < now()::date - interval '3 month'
               and date > now()::date - interval '3 month' - interval '1 week'
             order by code, date desc
         ),
     raw_data_1y as
         (
             select distinct on (
                 code
                 ) raw_data_3m.*,
                   historical_prices.date           as date_1y,
                   historical_prices.adjusted_close as price_1y
             from raw_data_3m
                      join {{ ref('historical_prices') }} using (code)
             where date < now()::date - interval '1 year'
               and date > now()::date - interval '1 year' - interval '1 week'
             order by code, date desc
         ),
     raw_data_13m as
         (
             select distinct on (
                 code
                 ) raw_data_1y.*,
                   historical_prices.date           as date_13m,
                   historical_prices.adjusted_close as price_13m
             from raw_data_1y
                      join {{ ref('historical_prices') }} using (code)
             where date < now()::date - interval '13 month'
               and date > now()::date - interval '13 month' - interval '1 week'
             order by code, date desc
         ),
     raw_data_5y as
         (
             select distinct on (
                 code
                 ) raw_data_13m.*,
                   historical_prices.date           as date_5y,
                   historical_prices.adjusted_close as price_5y
             from raw_data_13m
                      join {{ ref('historical_prices') }} using (code)
             where date < now()::date - interval '5 year'
               and date > now()::date - interval '5 year' - interval '1 week'
             order by code, date desc
         ),
     raw_data_all as
         (
             select distinct on (
                 code
                 ) raw_data_5y.*,
                   historical_prices.date           as date_all,
                   historical_prices.adjusted_close as price_all
             from raw_data_5y
                      join {{ ref('historical_prices') }} using (code)
             order by code, date
         )
select code as symbol,
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
