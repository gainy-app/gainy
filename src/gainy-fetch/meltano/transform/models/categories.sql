{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      index(this, 'id', true),
    ]
  )
}}

with categories (id, name, icon_url, risk_score) as
         (
             values (7, 'Momentum', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png', null),
                    (8, 'Value', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png', 1),
                    (1, 'Defensive', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png', 1),
                    (2, 'Speculation', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png', 3),
                    (5, 'Penny', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png', null),
                    (6, 'Dividend', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png', 2),
                    (3, 'ETF', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png', 1),
                    (4, 'Cryptocurrency', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png', 3),
                    (9, 'Growth', 'https://gainy-categories-dev.s3.amazonaws.com/placeholder.png', 2)
         )
SELECT *
from categories