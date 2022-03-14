{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "date__exchange_name") }} (date, exchange_name)',
    ]
  )
}}

SELECT CONCAT(dd::date, '_', exchanges.name)::varchar as id,
       dd::date                                       as date,
       exchanges.name                                 as exchange_name,
       coalesce(
               polygon_marketstatus_upcoming.open::timestamptz,
               (dd::date + exchanges.open_at::time) at time zone exchanges.timezone
           )                                          as open_at,
       coalesce(
               polygon_marketstatus_upcoming.close::timestamptz,
               (dd::date + exchanges.close_at::time) at time zone exchanges.timezone
           )                                          as close_at
FROM generate_series(now() - interval '1 week', now() + interval '1 week', interval '1 day') dd
         join {{ source('gainy', 'exchanges') }} on true
         left join {{ source('polygon', 'polygon_marketstatus_upcoming') }}
                   ON polygon_marketstatus_upcoming.exchange = exchanges.name
                       and polygon_marketstatus_upcoming.date::date = dd::date
where extract(isodow from dd) < 6
  and (polygon_marketstatus_upcoming.status is null or polygon_marketstatus_upcoming.status != 'closed')