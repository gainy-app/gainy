{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      index(this, 'id', true),
    ]
  )
}}


select (values (1))::int as id,
       (values (now()))  as last_activity_at