{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      index(this, 'id', true),
    ]
  )
}}

with categories (id, name, icon_url) as
         (
             values (7, 'Momentum', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png'),
                    (8, 'Value', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png'),
                    (1, 'Defensive', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png'),
                    (2, 'Speculation', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png'),
                    (5, 'Penny', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png'),
                    (6, 'Dividend', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png'),
                    (3, 'ETF', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png'),
                    (4, 'Cryptocurrency', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png'),
                    (9, 'Growth', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png')
         )
SELECT *
from categories