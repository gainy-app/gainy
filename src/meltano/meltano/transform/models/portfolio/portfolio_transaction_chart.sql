{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('transaction_uniq_id, period, datetime'),
      index('id', true),
      index('updated_at'),
    ],
  )
}}


{% if is_incremental() %}
with old_stats as
     (
         select max(updated_at) as max_updated_at
         from {{ this }}
     )
{% endif %}
select t.*,
       transaction_uniq_id || '_' || period || '_' || datetime as id
from (
         select portfolio_expanded_transactions.profile_id,
                portfolio_expanded_transactions.transaction_uniq_id,
                hpa.datetime::timestamp,
                hpa.date::date,
                '1d'                                                                               as period,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
                greatest(hpa.updated_at, portfolio_expanded_transactions.updated_at)               as updated_at
         from {{ ref('portfolio_expanded_transactions') }}
                  join {{ ref('historical_prices_aggregated_3min') }} hpa using(symbol)
{% if is_incremental() %}
                  join old_stats on true
{% endif %}
         where portfolio_expanded_transactions.is_app_trading = false
           and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '3 minutes' or
                portfolio_expanded_transactions.datetime is null)
{% if is_incremental() %}
           and (hpa.updated_at > max_updated_at or portfolio_expanded_transactions.updated_at > max_updated_at)
{% endif %}

         union all

         select portfolio_expanded_transactions.profile_id,
                portfolio_expanded_transactions.transaction_uniq_id,
                hpa.datetime::timestamp,
                hpa.date::date,
                '1w'                                                                               as period,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
                greatest(hpa.updated_at, portfolio_expanded_transactions.updated_at)               as updated_at
         from {{ ref('portfolio_expanded_transactions') }}
                  join {{ ref('historical_prices_aggregated_15min') }} hpa using(symbol)
{% if is_incremental() %}
                  join old_stats on true
{% endif %}
         where portfolio_expanded_transactions.is_app_trading = false
           and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '15 minutes' or
                portfolio_expanded_transactions.datetime is null)
{% if is_incremental() %}
           and (hpa.updated_at > max_updated_at or portfolio_expanded_transactions.updated_at > max_updated_at)
{% endif %}

         union all

         select portfolio_expanded_transactions.profile_id,
                portfolio_expanded_transactions.transaction_uniq_id,
                hpa.datetime::timestamp,
                hpa.datetime::date                                                                 as date,
                '1m'                                                                               as period,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
                greatest(hpa.updated_at, portfolio_expanded_transactions.updated_at)               as updated_at
         from {{ ref('portfolio_expanded_transactions') }}
                  join {{ ref('historical_prices_aggregated_1d') }} hpa using(symbol)
{% if is_incremental() %}
                  join old_stats on true
{% endif %}
         where hpa.datetime >= now() - interval '1 month + 1 week'
           and portfolio_expanded_transactions.is_app_trading = false
           and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '1 day' or
                portfolio_expanded_transactions.datetime is null)
{% if is_incremental() %}
           and (hpa.updated_at > max_updated_at or portfolio_expanded_transactions.updated_at > max_updated_at)
{% endif %}

         union all

         select portfolio_expanded_transactions.profile_id,
                portfolio_expanded_transactions.transaction_uniq_id,
                hpa.datetime::timestamp,
                hpa.datetime::date                                                                 as date,
                '3m'                                                                               as period,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
                greatest(hpa.updated_at, portfolio_expanded_transactions.updated_at)               as updated_at
         from {{ ref('portfolio_expanded_transactions') }}
                  join {{ ref('historical_prices_aggregated_1d') }} hpa using(symbol)
{% if is_incremental() %}
                  join old_stats on true
{% endif %}
         where hpa.datetime >= now() - interval '3 month + 1 week'
           and portfolio_expanded_transactions.is_app_trading = false
           and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '1 day' or
                portfolio_expanded_transactions.datetime is null)
{% if is_incremental() %}
           and (hpa.updated_at > max_updated_at or portfolio_expanded_transactions.updated_at > max_updated_at)
{% endif %}

         union all

         select portfolio_expanded_transactions.profile_id,
                portfolio_expanded_transactions.transaction_uniq_id,
                hpa.datetime::timestamp,
                hpa.datetime::date                                                                 as date,
                '1y'                                                                               as period,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
                greatest(hpa.updated_at, portfolio_expanded_transactions.updated_at)               as updated_at
         from {{ ref('portfolio_expanded_transactions') }}
                  join {{ ref('historical_prices_aggregated_1d') }} hpa using(symbol)
{% if is_incremental() %}
                  join old_stats on true
{% endif %}
         where portfolio_expanded_transactions.is_app_trading = false
           and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '1 day' or
                portfolio_expanded_transactions.datetime is null)
{% if is_incremental() %}
           and (hpa.updated_at > max_updated_at or portfolio_expanded_transactions.updated_at > max_updated_at)
{% endif %}

         union all

         select portfolio_expanded_transactions.profile_id,
                portfolio_expanded_transactions.transaction_uniq_id,
                hpa.datetime::timestamp,
                hpa.datetime::date                                                                 as date,
                '5y'                                                                               as period,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
                greatest(hpa.updated_at, portfolio_expanded_transactions.updated_at)               as updated_at
         from {{ ref('portfolio_expanded_transactions') }}
                  join {{ ref('historical_prices_aggregated_1w') }} hpa using(symbol)
{% if is_incremental() %}
                  join old_stats on true
{% endif %}
         where portfolio_expanded_transactions.is_app_trading = false
           and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '1 week' or
                portfolio_expanded_transactions.datetime is null)
{% if is_incremental() %}
           and (hpa.updated_at > max_updated_at or portfolio_expanded_transactions.updated_at > max_updated_at)
{% endif %}

         union all

         select portfolio_expanded_transactions.profile_id,
                portfolio_expanded_transactions.transaction_uniq_id,
                hpa.datetime::timestamp,
                hpa.datetime::date                                                                 as date,
                'all'                                                                              as period,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.open)           as open,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.high)           as high,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.low)            as low,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.close)          as close,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * hpa.adjusted_close) as adjusted_close,
                greatest(hpa.updated_at, portfolio_expanded_transactions.updated_at)               as updated_at
         from {{ ref('portfolio_expanded_transactions') }}
                  join {{ ref('historical_prices_aggregated_1m') }} hpa using(symbol)
{% if is_incremental() %}
                  join old_stats on true
{% endif %}
         where portfolio_expanded_transactions.is_app_trading = false
           and (hpa.datetime > portfolio_expanded_transactions.datetime - interval '1 month' or
                portfolio_expanded_transactions.datetime is null)
{% if is_incremental() %}
           and (hpa.updated_at > max_updated_at or portfolio_expanded_transactions.updated_at > max_updated_at)
{% endif %}

         union all

         select profile_id,
                transaction_uniq_id,
                datetime::timestamp,
                date::date,
                period::varchar,
                open,
                high,
                low,
                close,
                adjusted_close,
                updated_at
         from {{ ref('drivewealth_portfolio_chart') }}
{% if is_incremental() %}
                  join old_stats on true
         where updated_at > max_updated_at
{% endif %}
    ) t
{% if is_incremental() %}
         left join {{ this }} old_data using (transaction_uniq_id, period, datetime)
where old_data.transaction_uniq_id is null
   or abs(t.adjusted_close - old_data.adjusted_close) > 1e-3
{% endif %}
