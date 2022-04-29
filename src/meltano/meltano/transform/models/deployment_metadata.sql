{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('id'),
    ]
  )
}}


select (values (1))::int as id,
       (values (now()))  as last_activity_at