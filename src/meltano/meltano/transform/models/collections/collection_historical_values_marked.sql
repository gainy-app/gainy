{{
  config(
    materialized = "incremental",
    unique_key = "collection_uniq_id",
    post_hook=[
      pk('collection_uniq_id'),
    ]
  )
}}


with min_period_date as materialized
         (
             select collection_uniq_id, period, min(date) as min_date
             from {{ ref('collection_chart') }}
             group by collection_uniq_id, period
         ),
     data_1d as
         (
             select distinct on (
                 collection_uniq_id
                 ) collection_uniq_id,
                   date  as date_1d,
                   value as value_1d
             from {{ ref('collection_historical_values') }}
                      join min_period_date using (collection_uniq_id)
             where {{ ref('collection_historical_values') }}.date < min_date
               and period = '1d'
             order by collection_uniq_id desc, date desc
     ),
     data_1w as
         (
             select distinct on (
                 collection_uniq_id
                 ) collection_uniq_id,
                   date  as date_1w,
                   value as value_1w
             from {{ ref('collection_historical_values') }}
                      join min_period_date using (collection_uniq_id)
             where {{ ref('collection_historical_values') }}.date < min_date
               and period = '1w'
             order by collection_uniq_id desc, date desc
     ),
     data_1m as
         (
             select distinct on (
                 collection_uniq_id
                 ) collection_uniq_id,
                   date  as date_1m,
                   value as value_1m
             from {{ ref('collection_historical_values') }}
                      join min_period_date using (collection_uniq_id)
             where {{ ref('collection_historical_values') }}.date < min_date
               and period = '1m'
             order by collection_uniq_id desc, date desc
     ),
     data_3m as
         (
             select distinct on (
                 collection_uniq_id
                 ) collection_uniq_id,
                   date  as date_3m,
                   value as value_3m
             from {{ ref('collection_historical_values') }}
                      join min_period_date using (collection_uniq_id)
             where {{ ref('collection_historical_values') }}.date < min_date
               and period = '3m'
             order by collection_uniq_id desc, date desc
     ),
     data_1y as
         (
             select distinct on (
                 collection_uniq_id
                 ) collection_uniq_id,
                   date  as date_1y,
                   value as value_1y
             from {{ ref('collection_historical_values') }}
                      join min_period_date using (collection_uniq_id)
             where {{ ref('collection_historical_values') }}.date < min_date
               and period = '1y'
             order by collection_uniq_id desc, date desc
     ),
     data_5y as
         (
             select distinct on (
                 collection_uniq_id
                 ) collection_uniq_id,
                   date  as date_5y,
                   value as value_5y
             from {{ ref('collection_historical_values') }}
                      join min_period_date using (collection_uniq_id)
             where {{ ref('collection_historical_values') }}.date < min_date
               and period = '5y'
             order by collection_uniq_id desc, date desc
     ),
     data_total as
         (
             select distinct on (
                 collection_uniq_id
                 ) collection_uniq_id,
                   date  as date_all,
                   value as value_all
             from {{ ref('collection_historical_values') }}
             order by collection_uniq_id, date
     )
select *
from data_1d
         left join data_1w using (collection_uniq_id)
         left join data_1m using (collection_uniq_id)
         left join data_3m using (collection_uniq_id)
         left join data_1y using (collection_uniq_id)
         left join data_5y using (collection_uniq_id)
         left join data_total using (collection_uniq_id)
