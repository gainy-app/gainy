{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol, component_symbol'),
      index(this, 'id', true),
      'delete from {{ this }}
        using (select distinct on (symbol) symbol, version from {{ this }} order by symbol, updated_at desc) old_version
        where ticker_components.symbol = old_version.symbol
          and ticker_components.version != old_version.version',
    ]
  )
}}


with
{% if is_incremental() %}
     old_version as (select distinct on (symbol) symbol as code, version from {{ this }} order by code, updated_at desc),
{% endif %}
     versioned_data as
         (
             select code,
                    etf_data -> 'Holdings'             as holdings,
                    jsonb_hash(etf_data -> 'Holdings') as version,
                    case
                        when is_date(updatedat)
                            then updatedat::timestamp
                        else _sdc_batched_at
                        end                            as updated_at
             from {{ source('eod', 'eod_fundamentals') }}
             where type = 'ETF'
               and etf_data is not null
         ),
     expanded as
         (
             select code,
                    (json_each((holdings)::json)).*,
                    version,
                    updated_at
             from versioned_data
         ),
     data as
         (
             select expanded.code                                  as symbol,
                    expanded.key                                   as original_component_symbol,
                    expanded.value ->> 'Code'                      as component_symbol,
                    expanded.value ->> 'Name'                      as component_name,
                    expanded.value ->> 'Region'                    as component_region,
                    expanded.value ->> 'Sector'                    as component_sector,
                    expanded.value ->> 'Country'                   as component_country,
                    (expanded.value ->> 'Assets_%')::numeric / 100 as component_weight,
                    expanded.value ->> 'Exchange'                  as component_exchange,
                    expanded.value ->> 'Industry'                  as component_industry,
                    expanded.version,
                    expanded.updated_at
             from expanded
                 join {{ ref('tickers') }} on tickers.symbol = (expanded.value ->> 'Code')
             {% if is_incremental() %}
                 left join old_version using (code)
             {% endif %}

             where tickers.name = (expanded.value ->> 'Name')

             {% if is_incremental() %}
               and expanded.version != old_version.version or old_version is null
             {% endif %}
         )
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
from data
group by symbol, component_symbol
