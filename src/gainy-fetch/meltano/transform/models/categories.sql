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
             values
                 (1, 'Defensive', 'https://gainy-categories-production.s3.amazonaws.com/Defensive%403x.png', 1),
                 (2, 'Speculation', 'https://gainy-categories-production.s3.amazonaws.com/Speculation%403x.png', 3),
                 (3, 'ETF', 'https://gainy-categories-production.s3.amazonaws.com/ETF%403x.png', 1),
                 (4, 'Cryptocurrency', 'https://gainy-categories-production.s3.amazonaws.com/Cryptocurrency%403x.png', 3),
                 (5, 'Penny', 'https://gainy-categories-production.s3.amazonaws.com/Penny%403x.png', 3),
                 (6, 'Dividend', 'https://gainy-categories-production.s3.amazonaws.com/Dividend%403x.png', 2),
                 (7, 'Momentum', 'https://gainy-categories-production.s3.amazonaws.com/Momentum%403x.png', 3),
                 (8, 'Value', 'https://gainy-categories-production.s3.amazonaws.com/Value%403x.png', 1),
                 (9, 'Growth', 'https://gainy-categories-production.s3.amazonaws.com/Growth%403x.png', 2)
         )
SELECT *
from categories
