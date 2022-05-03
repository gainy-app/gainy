{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook = [
      index(this, 'id', true),
      index(this, 'collection_id', false),
      'create unique index if not exists "symbol__collection_id" ON {{ this }} (symbol, collection_id)',
      'delete from {{this}} where created_at < (select max(created_at) from {{this}})',
      'update {{ ref("collections") }} c set size = collection_sizes.size from (select collection_id, count(*) as size from {{this}} GROUP BY collection_id) collection_sizes where collection_sizes.collection_id = c.id',
    ]
  )
}}

/* THE MODEL FILE IS AUTO GENERATED DURING BUILD, YOU SHOULD NOT EDIT THE MODEL, EDIT THE TEMPLATE INSTEAD  */

with historical_prices as (select * from {{ ref('historical_prices') }}),
     tickers as (select * from {{ ref('tickers') }}),
     ticker_interests as (select * from {{ ref('ticker_interests') }}),
     interests as (select * from {{ ref('interests') }}),
     ticker_custom_industries as (select * from {{ ref('ticker_custom_industries') }}),
     gainy_industries as (select * from {{ ref('gainy_industries') }}),
     ticker_categories as (select * from {{ ref('ticker_categories') }}),
     categories as (select * from {{ ref('categories') }}),
     collections as (select id::int, name from {{ ref('collections') }} where personalized = '0'),
     countries as (select *
                   from {{ source('gainy', 'gainy_countries') }}
                   where _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ source('gainy', 'gainy_countries') }}) - interval '1 minute'),
     ticker_metrics as (select * from {{ ref('ticker_metrics') }}),
     latest_price AS
         (
             select distinct on (hp.code) hp.*
             from historical_prices hp
             where close is not null
               AND open is not null
               and date > NOW() - interval '1 week'
             order by hp.code, hp.date DESC --we get latest by date not-null prices (if any)
         ),
     t_ranks AS
        (
             select tm.symbol,
                    ROW_NUMBER() OVER(partition by t.type order by tm.avg_volume_10d desc)     as vol_10d_topintype_order,
                    (ROW_NUMBER() OVER(partition by t.type order by tm.avg_volume_10d desc)) /
                    (count(*)     OVER(partition by t.type))::float                            as vol_10d_topintype_percent
             from ticker_metrics tm
             JOIN tickers t on t.symbol = tm.symbol
        ),
     ct as
         (
             select t.symbol                                  as ticker_code,
                    t.country_name,
                    t.ipo_date,
                    latest_price.close                        as price,
                    CASE
                        WHEN latest_price.close > 0 AND latest_price.open > 0
                            THEN (latest_price.close - latest_price.open) / latest_price.open --else NULL
                        END                                   as chrt_1d,
                    ticker_metrics.price_change_1m            as chrt_1m,
                    ticker_metrics.price_change_1y            as chrt_1y,
                    t.type                                    as ttype,
                    t.gic_sector                              as gics_sector,
                    t.sector                                  as eod_sector,
                    gainy_industries.name                     as g_industry,
                    interests.name                            as g_interest,
                    lower(c.name)                             as investcat,
                    CASE
                        WHEN countries.region = 'Europe' THEN 'europe'
                        WHEN countries."sub-region" LIKE '%Latin America%' THEN 'latam'
                        END                                   as country_group,
                    ticker_metrics.short_percent,
                    t_ranks.vol_10d_topintype_order,
                    t_ranks.vol_10d_topintype_percent
             from tickers t
                      LEFT JOIN ticker_custom_industries on t.symbol = ticker_custom_industries.symbol --here we have N:N relationship, so we must use distinct in the end (we will get duplicates otherwise)
                      LEFT JOIN gainy_industries on ticker_custom_industries.industry_id = gainy_industries.id
                      LEFT JOIN ticker_interests on t.symbol = ticker_interests.symbol --here we have N:N relationship, so we must use distinct in the end (we will get duplicates otherwise)
                      LEFT JOIN interests on ticker_interests.interest_id = interests.id
                      LEFT JOIN latest_price ON latest_price.code = t.symbol           
                      LEFT JOIN ticker_categories tc
                                on t.symbol = tc.symbol --here we have N:N relationship, so we must use distinct in the end (we will get duplicates otherwise)
                      LEFT JOIN categories c on tc.category_id = c.id
                      LEFT JOIN countries
                                on countries.name = t.country_name OR countries."alpha-2" = t.country_name OR
                                   countries."alpha-3" = t.country_name
                      LEFT JOIN t_ranks on t_ranks.symbol = t.symbol
                      LEFT JOIN ticker_metrics on ticker_metrics.symbol = t.symbol
         ),
     tmp_ticker_collections as
         (
-- __SELECT__ --
         )
SELECT distinct CONCAT(t2.symbol, '_', collection_id::varchar)::varchar as id,
                t2.symbol,
                collection_id,
                NOW() as created_at
from tmp_ticker_collections
         join tickers t2 on tmp_ticker_collections.symbol = t2.symbol
         join collections c2 on tmp_ticker_collections.collection_id = c2.id
