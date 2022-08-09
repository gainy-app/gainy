{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol, component_symbol'),
      index(this, 'id', true),
      'delete from {{ this }}
        using (select distinct on (symbol) symbol, version from {{ this }} order by symbol, updated_at desc) old_version
        where ticker_components_flat.symbol = old_version.symbol
          and ticker_components_flat.version != old_version.version',
    ]
  )
}}


with
{% if is_incremental() %}
     old_version as (select distinct on (symbol) symbol, version from {{ this }} order by symbol, updated_at desc),
{% endif %}
     ticker_components_flat0 as
         (
             select ticker_components.symbol,
                    case
                        when component_ticker_components.component_symbol is not null
                            then ticker_components.component_symbol
                        end                                               as through_symbol,
                    coalesce(component_ticker_components.original_component_symbol,
                             ticker_components.original_component_symbol) as original_component_symbol,
                    coalesce(component_ticker_components.component_symbol,
                             ticker_components.component_symbol)          as component_symbol,
                    coalesce(component_ticker_components.component_name,
                             ticker_components.component_name)            as component_name,
                    coalesce(component_ticker_components.component_region,
                             ticker_components.component_region)          as component_region,
                    coalesce(component_ticker_components.component_sector,
                             ticker_components.component_sector)          as component_sector,
                    coalesce(component_ticker_components.component_country,
                             ticker_components.component_country)         as component_country,
                    coalesce(component_ticker_components.component_weight, 1) *
                    ticker_components.component_weight                    as component_weight,
                    coalesce(component_ticker_components.component_exchange,
                             ticker_components.component_exchange)        as component_exchange,
                    coalesce(component_ticker_components.component_industry,
                             ticker_components.component_industry)        as component_industry,
                    ticker_components.version,
                    coalesce(component_ticker_components.updated_at,
                             ticker_components.updated_at)                as updated_at
             from {{ ref('ticker_components') }}
                      left join {{ ref('ticker_components') }} component_ticker_components
                                on component_ticker_components.symbol = ticker_components.component_symbol
                  -- eliminate loops when etf owns itself
             where component_ticker_components is null
                or component_ticker_components.component_symbol != component_ticker_components.symbol
         ),
     ticker_components_flat1 as
         (
             select ticker_components_flat0.symbol,
                    case
                        when component_ticker_components.component_symbol is not null
                            then coalesce(ticker_components_flat0.through_symbol || ',', '') ||
                                 ticker_components_flat0.component_symbol
                        else ticker_components_flat0.through_symbol
                        end                                                     as through_symbol,
                    coalesce(component_ticker_components.original_component_symbol,
                             ticker_components_flat0.original_component_symbol) as original_component_symbol,
                    coalesce(component_ticker_components.component_symbol,
                             ticker_components_flat0.component_symbol)          as component_symbol,
                    coalesce(component_ticker_components.component_name,
                             ticker_components_flat0.component_name)            as component_name,
                    coalesce(component_ticker_components.component_region,
                             ticker_components_flat0.component_region)          as component_region,
                    coalesce(component_ticker_components.component_sector,
                             ticker_components_flat0.component_sector)          as component_sector,
                    coalesce(component_ticker_components.component_country,
                             ticker_components_flat0.component_country)         as component_country,
                    coalesce(component_ticker_components.component_weight, 1) *
                    ticker_components_flat0.component_weight                    as component_weight,
                    coalesce(component_ticker_components.component_exchange,
                             ticker_components_flat0.component_exchange)        as component_exchange,
                    coalesce(component_ticker_components.component_industry,
                             ticker_components_flat0.component_industry)        as component_industry,
                    ticker_components_flat0.version,
                    coalesce(component_ticker_components.updated_at,
                             ticker_components_flat0.updated_at)                as updated_at
             from ticker_components_flat0
                      left join {{ ref('ticker_components') }} component_ticker_components
                                on component_ticker_components.symbol = ticker_components_flat0.component_symbol
             where component_ticker_components is null
                or component_ticker_components.component_symbol != component_ticker_components.symbol
         ),
    data_groupped as
         (
             select symbol,
                    min(original_component_symbol)          as original_component_symbol,
                    component_symbol,
                    min(component_name)                     as component_name,
                    min(component_region)                   as component_region,
                    min(component_sector)                   as component_sector,
                    min(component_country)                  as component_country,
                    sum(component_weight)                   as component_weight,
                    sum(component_weight)::double precision as component_weight_float,
                    min(component_exchange)                 as component_exchange,
                    min(component_industry)                 as component_industry,
                    symbol || '_' || component_symbol       as id,
                    min(version)                            as version,
                    min(updated_at)                         as updated_at
             from ticker_components_flat1
             group by symbol, component_symbol
         )
select data_groupped.*
from data_groupped
{% if is_incremental() %}
         left join old_version using (symbol)
where data_groupped.version != old_version.version or old_version is null
{% endif %}
