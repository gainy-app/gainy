{{
  config(
    materialized = "incremental",
    unique_key = "collection_uniq_id",
    post_hook=[
      pk('collection_uniq_id'),
    ]
  )
}}

with raw_data_0d as
         (
             select collection_uniq_id,
                    date  as date_0d,
                    value as value_0d,
                    updated_at
             from collection_historical_values
                      join (
                               select collection_uniq_id,
                                      max(collection_historical_values.date) as date
                               from collection_historical_values
                               group by collection_uniq_id
                           ) t
                           using (collection_uniq_id, date)
         ),
     raw_data_1w as
         (
             select raw_data_0d.*,
                    collection_historical_values.date  as date_1w,
                    collection_historical_values.value as value_1w
             from raw_data_0d
                      left join (
                                    select collection_uniq_id,
                                           max(collection_historical_values.date) as date
                                    from collection_historical_values
                                    where date < now()::date - interval '1 week'
                                    group by collection_uniq_id
                                ) t
                                using (collection_uniq_id)
                      left join collection_historical_values using (collection_uniq_id, date)
     ),
     raw_data_10d as
         (
             select distinct on (
                 raw_data_1w.collection_uniq_id
                 ) raw_data_1w.*,
                   collection_historical_values.date  as date_10d,
                   collection_historical_values.value as value_10d
             from raw_data_1w
                      left join collection_historical_values
                                on collection_historical_values.collection_uniq_id = raw_data_1w.collection_uniq_id
                                    and date < now()::date - interval '10 days'
                                    and date > now()::date - interval '10 days' - interval '1 week'
             order by collection_uniq_id, date desc
     ),
     raw_data_1m as
         (
             select distinct on (
                 raw_data_10d.collection_uniq_id
                 ) raw_data_10d.*,
                   collection_historical_values.date  as date_1m,
                   collection_historical_values.value as value_1m
             from raw_data_10d
                      left join collection_historical_values
                                on collection_historical_values.collection_uniq_id = raw_data_10d.collection_uniq_id
                                    and date < now()::date - interval '1 month'
                                    and date > now()::date - interval '1 month' - interval '1 week'
             order by collection_uniq_id, date desc
     ),
     raw_data_2m as
         (
             select distinct on (
                 raw_data_1m.collection_uniq_id
                 ) raw_data_1m.*,
                   collection_historical_values.date  as date_2m,
                   collection_historical_values.value as value_2m
             from raw_data_1m
                      left join collection_historical_values
                                on collection_historical_values.collection_uniq_id = raw_data_1m.collection_uniq_id
                                    and date < now()::date - interval '2 month'
                                    and date > now()::date - interval '2 month' - interval '1 week'
             order by collection_uniq_id, date desc
     ),
     raw_data_3m as
         (
             select distinct on (
                 raw_data_2m.collection_uniq_id
                 ) raw_data_2m.*,
                   collection_historical_values.date  as date_3m,
                   collection_historical_values.value as value_3m
             from raw_data_2m
                      left join collection_historical_values
                                on collection_historical_values.collection_uniq_id = raw_data_2m.collection_uniq_id
                                    and date < now()::date - interval '3 month'
                                    and date > now()::date - interval '3 month' - interval '1 week'
             order by collection_uniq_id, date desc
     ),
     raw_data_1y as
         (
             select raw_data_3m.*,
                    collection_historical_values.date  as date_1y,
                    collection_historical_values.value as value_1y
             from raw_data_3m
                      left join (
                                    select collection_uniq_id,
                                           max(collection_historical_values.date) as date
                                    from collection_historical_values
                                    where date < now()::date - interval '1 year'
                                    group by collection_uniq_id
                                ) t using (collection_uniq_id)
                      left join collection_historical_values using (collection_uniq_id, date)
     ),
     raw_data_13m as
         (
             select distinct on (
                 raw_data_1y.collection_uniq_id
                 ) raw_data_1y.*,
                   collection_historical_values.date  as date_13m,
                   collection_historical_values.value as value_13m
             from raw_data_1y
                      left join collection_historical_values
                                on collection_historical_values.collection_uniq_id = raw_data_1y.collection_uniq_id
                                    and date < now()::date - interval '13 month'
                                    and date > now()::date - interval '13 month' - interval '1 week'
             order by collection_uniq_id, date desc
     ),
     raw_data_5y as
         (
             select distinct on (
                 raw_data_13m.collection_uniq_id
                 ) raw_data_13m.*,
                   collection_historical_values.date  as date_5y,
                   collection_historical_values.value as value_5y
             from raw_data_13m
                      left join collection_historical_values
                                on collection_historical_values.collection_uniq_id = raw_data_13m.collection_uniq_id
                                    and date < now()::date - interval '5 year'
                                    and date > now()::date - interval '5 year' - interval '1 week'
             order by collection_uniq_id, date desc
     ),
     raw_data_all as
         (
             select raw_data_5y.*,
                    collection_historical_values.date  as date_all,
                    collection_historical_values.value as value_all
             from raw_data_5y
                      join (
                               select collection_uniq_id,
                                      min(collection_historical_values.date) as date
                               from collection_historical_values
                               group by collection_uniq_id
                           ) t
                           using (collection_uniq_id)
                      join collection_historical_values using (collection_uniq_id, date)
     )
select collection_uniq_id,
       date_0d,
       value_0d,
       date_1w,
       value_1w,
       date_10d,
       value_10d,
       date_1m,
       value_1m,
       date_2m,
       value_2m,
       date_3m,
       value_3m,
       date_1y,
       value_1y,
       date_13m,
       value_13m,
       date_5y,
       value_5y,
       date_all,
       value_all,
       updated_at
from raw_data_all
