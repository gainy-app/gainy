{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('id'),
      'delete from {{this}}
        using (select period, max(updated_at) as max_updated_at from {{this}} group by period) dc_stats
        where dc_stats.period = data_checks.period
        and data_checks.updated_at < dc_stats.max_updated_at',
    ]
  )
}}


with collection_distinct_tickers as
         (
             select distinct symbol
             from {{ ref('ticker_collections') }}
                      join {{ ref('collections') }} on ticker_collections.collection_id = collections.id
             where collections.enabled = '1'
               and collections.personalized = '0'
         ),
     latest_trading_day as
         (
             select distinct on (exchange_name, country_name) *
             from {{ ref('exchange_schedule') }}
             where open_at < now()
             order by exchange_name, country_name, date desc
         ),
     tickers_and_options as
         (
             select symbol, exchange_canonical, country_name
             from {{ ref('tickers') }}
             union all
             select contract_name, exchange_canonical, country_name
             from {{ ref('ticker_options_monitored') }}
                      join {{ ref('tickers') }} using (symbol)
         ),
     old_historical_prices as
         (
             select symbol
             from (
                      select tickers.symbol,
                             max(historical_prices.date) as date
                      from {{ ref('tickers') }}
                               left join {{ ref('historical_prices') }} on historical_prices.code = tickers.symbol
                      where volume > 0
                      group by tickers.symbol
                  ) t
                      join {{ ref('tickers') }} using (symbol)
                      join latest_trading_day
                           on (latest_trading_day.exchange_name = tickers.exchange_canonical
                               or (tickers.exchange_canonical is null
                                   and latest_trading_day.country_name = tickers.country_name))
             where t.date is null
                or latest_trading_day.date - t.date > (now() < latest_trading_day.date::timestamp + interval '28 hours')::int
-- US markets typically close at 20:00. Fetching starts at 2:00. We expect prices to be present in 2 hours after start.
         ),
     old_realtime_prices as
         (
             select symbol
             from (
                      select tickers.symbol,
                             max(eod_intraday_prices.time) as time
                      from {{ ref('tickers') }}
                               left join {{ source('eod', 'eod_intraday_prices') }} using (symbol)
                      group by tickers.symbol
                  ) t
                      left join old_historical_prices using (symbol)
                      join {{ ref('tickers') }} using (symbol)
                      join latest_trading_day
                           on (latest_trading_day.exchange_name = tickers.exchange_canonical
                               or (tickers.exchange_canonical is null
                                   and latest_trading_day.country_name = tickers.country_name))
             where old_historical_prices.symbol is null
               and (t.time is null or least(now(), latest_trading_day.close_at) - t.time > interval '16 minutes')
-- Polygon delay is 15 minutes
         ),
     matchscore_distinct_tickers as
         (
             select symbol
             from {{ source('app', 'profile_ticker_match_score') }}
             group by symbol
         ),
     errors as
         (
{% if not var('realtime') %}
             select symbol,
                    'ttf_ticker_no_interest' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('ticker_interests') }} using (symbol)
                      left join {{ ref('interests') }} on interests.id = ticker_interests.interest_id
             where interests.id is null

             union all

             select symbol,
                   'ttf_ticker_no_industry' as code,
                   'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('ticker_industries') }} using (symbol)
                      left join {{ ref('gainy_industries') }} on gainy_industries.id = ticker_industries.industry_id
             where gainy_industries.id is null

             union all

             select collection_distinct_tickers.symbol,
                    'ttf_ticker_hidden' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('tickers') }} using (symbol)
             where tickers.symbol is null
           
             union all
           
             select collection_distinct_tickers.symbol,
                    'ttf_ticker_no_risk_score' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('ticker_risk_scores') }} using (symbol)
             where ticker_risk_scores.symbol is null
           
             union all
           
             select collection_distinct_tickers.symbol,
                    'ttf_ticker_no_category_continuous' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('ticker_categories_continuous') }} using (symbol)
             where ticker_categories_continuous.symbol is null

             union all
           
             select collection_distinct_tickers.symbol,
                    'ttf_ticker_no_matchscore' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join matchscore_distinct_tickers using (symbol)
                               left join {{ ref('tickers') }} using (symbol)
             where matchscore_distinct_tickers.symbol is null
             and tickers.type <> 'crypto'

             union all

             select symbol,
                    'old_historical_prices' as code,
                    'daily' as period
             from old_historical_prices

             union all
{% endif %}
             select tickers_and_options.symbol,
                    'old_realtime_metrics' as code,
                    'realtime' as period
             from tickers_and_options
                      join latest_trading_day
                           on (latest_trading_day.exchange_name = tickers_and_options.exchange_canonical
                               or (tickers_and_options.exchange_canonical is null
                                   and latest_trading_day.country_name = tickers_and_options.country_name))
                      left join {{ ref('ticker_realtime_metrics') }} on ticker_realtime_metrics.symbol = tickers_and_options.symbol
             where ticker_realtime_metrics.symbol is null
                or least(now(), latest_trading_day.close_at) - ticker_realtime_metrics.time > interval '30 minutes'

             union all

             select symbol,
                    'old_realtime_chart' as code,
                    'realtime' as period
             from (
                      select tickers_and_options.symbol,
                             max(chart.datetime) as datetime
                      from tickers_and_options
                               left join {{ ref('chart') }}
                                         on chart.symbol = tickers_and_options.symbol
                                             and chart.period = '1d'
                      group by tickers_and_options.symbol
                  ) t
                      left join old_historical_prices using (symbol)
                      left join old_realtime_prices using (symbol)
                      join tickers_and_options using (symbol)
                      join latest_trading_day
                           on (latest_trading_day.exchange_name = tickers_and_options.exchange_canonical
                               or (tickers_and_options.exchange_canonical is null
                                   and latest_trading_day.country_name = tickers_and_options.country_name))
             where old_historical_prices.symbol is null
               and old_realtime_prices.symbol is null
               and (datetime is null or least(now(), latest_trading_day.close_at) - datetime > interval '30 minutes')

             union all

             select symbol,
                    'old_realtime_prices' as code,
                    'realtime' as period
             from old_realtime_prices
         )
select (code || '_' || symbol)::varchar as id,
       symbol,
       code::varchar,
       period::varchar,
       case
           when code = 'ttf_ticker_no_interest'
               then 'TTF tickers ' || symbol || ' is not linked to any interest.'
           when code = 'ttf_ticker_no_industry'
               then 'TTF tickers ' || symbol || ' is not linked to any industry.'
           when code = 'ttf_ticker_hidden'
               then 'TTF tickers ' || symbol || ' not present in the tickers table.'
           when code = 'ttf_ticker_no_risk_score'
               then 'TTF tickers ' || symbol || ' not present in the ticker_risk_scores table.'
           when code = 'ttf_ticker_no_category_continuous'
               then 'TTF tickers ' || symbol || ' not present in the ticker_categories_continuous table.'
           when code = 'ttf_ticker_no_matchscore'
               then 'TTF tickers ' || symbol || ' not present in the app.profile_ticker_match_score table.'
           when code = 'old_realtime_metrics'
               then 'Tickers ' || symbol || ' has old realtime metrics.'
           when code = 'old_realtime_chart'
               then 'Tickers ' || symbol || ' has old realtime chart.'
           when code = 'old_historical_prices'
               then 'Tickers ' || symbol || ' has old historical prices.'
           when code = 'old_realtime_prices'
               then 'Tickers ' || symbol || ' has old realtime prices.'
           end as message,
       now() as updated_at
from errors



{% if not var('realtime') %}

union all
	(
		with
		check_params(		table_name, 				dt_interval, 
					depth_stddev, 				depth_check, -- depth_stddev >= depth_check !!
					allowable_sigma_dev_adjclose_percchange, 	allowable_sigma_dev_volume_dif, 
					symbol_asmarket_crypto, 		symbol_asmarket_other) as
			(
				values 	('raw_data.eod_historical_prices', 	interval '1 day', 
					interval '5 year', 			interval '5 year', 
					5., 					10., 
					'BTC.CC', 				'SPY')
			),
		
		intrpl_dt_now_wrt_interval as -- generating time frames
			(	-- now()='2022-06-22 15:15:14' with dt_interval='15 minutes'::interval will give: '2022-06-22 15:15:00' (it drops last not full frame)
				select 	(to_timestamp(
							(extract(epoch from now()::timestamp)::int / extract(epoch from cp.dt_interval)::int
							) * extract(epoch from cp.dt_interval)::int
						) AT TIME ZONE 'UTC' --to cancel out TZ in to_timestamp(0)='1970-01-01 00:00:00+TZ' (will be shifted by 3 hrs if TZ was +3, ie '1970-01-01 03:00:00+03')
					)::timestamp as value
				from check_params cp
			),
		
		intrpl_symbol_asmarket_dt as 
			(
				select 	unnest(array[cp.symbol_asmarket_other, cp.symbol_asmarket_crypto]) as intrpl_symbol_asmarket,
					d.dt::timestamp
				from check_params cp
				left join intrpl_dt_now_wrt_interval on true
				cross join 
					generate_series(intrpl_dt_now_wrt_interval.value - cp.depth_stddev, 
							intrpl_dt_now_wrt_interval.value,
							cp.dt_interval) as d(dt)
			),
			
		/* 
		 * for cancel natural market price movements in measuring ticker anomaly price movements by "returns" (percentage price change d2d)
		 * we need to first subtract market returns from ticker returns (and only after that do some stat measurements by stdev of returns)
		 * so we need to guarantee that market(target) price will be presented in any dt that can occur in any ticker dt 
		 * and if we haven't row for market price when the ticker has - that mean we lost the market price data row 
		 * so we need to guarantee some reasonable intermediate value. The linear interpolation trick is all about it.
		 */
		intrpl_symbol_asmarket_dt_prices as 
			(
				select 	d.intrpl_symbol_asmarket,
					d.dt, 
					--t.adjusted_close as adjusted_close_raw, --have left it here if anyone wants to check
					coalesce(t.adjusted_close, 
						linear_interpolate(
							extract(epoch from d.dt),
							LAST_VALUE_IGNORENULLS(case when t.adjusted_close is not null then extract(epoch from d.dt) else null end) over (lookback),
							LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookback),
							LAST_VALUE_IGNORENULLS(case when t.adjusted_close is not null then extract(epoch from d.dt) else null end) over (lookforward),
							LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookforward)
							),
						LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookforward),
						LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookback),
						0) as intrpl_symbol_asmarket_adjusted_close
				from intrpl_symbol_asmarket_dt as d
				left join 
					(
						select 	code as intrpl_symbol_asmarket,
							"date"::timestamp as dt,
							adjusted_close
						from {{ source('eod', 'eod_historical_prices') }} ehp
						join check_params cp on true
						where code = any(array[cp.symbol_asmarket_other, cp.symbol_asmarket_crypto])
					) as t using(intrpl_symbol_asmarket, dt)
				window
					lookback as (partition by d.intrpl_symbol_asmarket order by d.dt asc),
					lookforward as (partition by d.intrpl_symbol_asmarket order by d.dt desc)
			),
		
		tickers_data as
			(
				select 
					code 										as symbol, 
					to_timestamp("date",'YYYY-MM-DD')::timestamp 					as dt,
					adjusted_close,
					volume,
					case 	when t."type" = 'crypto' then cp.symbol_asmarket_crypto 
						else cp.symbol_asmarket_other end					as intrpl_symbol_asmarket
				from {{ source('eod', 'eod_historical_prices') }} ehp
				join {{ ref('tickers') }} t on t.symbol = ehp.code -- we are interested in all tickers that are available in the app, so tickers table
				left join check_params cp on true 
				where to_timestamp("date",'YYYY-MM-DD')::timestamp >= now()::timestamp - cp.depth_stddev
			),
		tickers_lag as
			(
				select 
					t.*, -- symbol, dt, adjusted_close, volume, intrpl_symbol_asmarket
					lag(t.adjusted_close,1,t.adjusted_close) over (partition by t.symbol order by t.dt asc) 	as adjusted_close_pre,
					lag(t.volume,1,t.volume) over (partition by t.symbol order by t.dt asc) 			as volume_pre,
					sam.intrpl_symbol_asmarket_adjusted_close,
					lag(sam.intrpl_symbol_asmarket_adjusted_close,1,sam.intrpl_symbol_asmarket_adjusted_close) over (partition by t.symbol order by t.dt asc) 	as intrpl_symbol_asmarket_adjusted_close_pre
				from tickers_data t
				join intrpl_symbol_asmarket_dt_prices sam using (intrpl_symbol_asmarket, dt)
			),
		tickers_difs as
			(
				select
					*,
					adjusted_close - adjusted_close_pre 		as adjusted_close_dif,
					(adjusted_close - adjusted_close_pre)
						/(1e-30 + abs(adjusted_close_pre)) 	as adjusted_close_perc_change,
					volume - volume_pre 				as volume_dif,
					intrpl_symbol_asmarket_adjusted_close - intrpl_symbol_asmarket_adjusted_close_pre 	as intrpl_symbol_asmarket_adjusted_close_dif,
					(intrpl_symbol_asmarket_adjusted_close - intrpl_symbol_asmarket_adjusted_close_pre)
						/(1e-30 + abs(intrpl_symbol_asmarket_adjusted_close_pre))			as intrpl_symbol_asmarket_adjusted_close_perc_change,
					(adjusted_close - adjusted_close_pre)
						/(1e-30 + abs(adjusted_close_pre))
					- (intrpl_symbol_asmarket_adjusted_close - intrpl_symbol_asmarket_adjusted_close_pre)
						/(1e-30 + abs(intrpl_symbol_asmarket_adjusted_close_pre))			as adjusted_close_perc_change_wom
				from tickers_lag 	
			),
		tickers_stddevs_means_devs as
			(
				select 
					*,
					stddev_pop(adjusted_close_perc_change_wom) over (w_s) 	as stddev_adjusted_close_perc_change_wom,
					avg(adjusted_close_perc_change_wom) over (w_s) 		as mean_adjusted_close_perc_change_wom,
					abs(adjusted_close_perc_change_wom 
						- avg(adjusted_close_perc_change_wom) over (w_s)) 	as dev_adjusted_close_perc_change_wom,
					stddev_pop(volume_dif) over (w_s) 			as stddev_volume_dif,
					avg(volume_dif) over (w_s) 				as mean_volume_dif,
					abs(volume_dif 
						- avg(volume_dif) over (w_s)) 			as dev_volume_dif
				from tickers_difs
				window w_s as (partition by symbol)
			),
		tickers_checks as 
			(
				select 
					*,
					case when dev_adjusted_close_perc_change_wom > 
						stddev_adjusted_close_perc_change_wom * cp.allowable_sigma_dev_adjclose_percchange
												then 1 else 0 end 	as iserror_adjusted_close_perc_change_dev_wom,
					case when dev_volume_dif > 
						stddev_volume_dif * cp.allowable_sigma_dev_volume_dif
												then 1 else 0 end 	as iserror_volume_dif_dev,
					case when adjusted_close = adjusted_close_pre 		then 1 else 0 end 	as iserror_adjusted_close_twice_same,
					case when adjusted_close <= 0 				then 1 else 0 end 	as iserror_adjusted_close_is_notpositive,
					case when adjusted_close is null 			then 1 else 0 end 	as iserror_adjusted_close_is_null,
					case when volume is null 				then 1 else 0 end 	as iserror_volume_is_null
				from tickers_stddevs_means_devs
				left join check_params cp on true
				where dt >= now()::timestamp - cp.depth_check
			),
		tickers_checks_agg as -- I have left all the fields here, so someone could see them closer (copy and past all code up to here and see the output)
			(
				select 
					*, 
					sum(iserror_adjusted_close_perc_change_dev_wom) over (w_s) 	as iserror_adjusted_close_perc_change_dev_wom_agg, 	-- agg >0 && distinct on (symbol) -> example case
					sum(iserror_volume_dif_dev) over (w_s) 				as iserror_volume_dif_dev_agg, 				-- agg >0 && distinct on (symbol) -> example case
					sum(iserror_adjusted_close_twice_same) over (w_s) 		as iserror_adjusted_close_twice_same_agg, 		-- agg >0 && distinct on (symbol) -> example case
					sum(iserror_adjusted_close_is_notpositive) over (w_s) 		as iserror_adjusted_close_is_notpositive_agg, 		-- agg >0 && distinct on (symbol) -> example case
					sum(iserror_adjusted_close_is_null) over (w_s) 			as iserror_adjusted_close_is_null_agg, 			-- agg >0 && distinct on (symbol) -> example case
					sum(iserror_volume_is_null) over (w_s) 				as iserror_volume_is_null_agg, 				-- agg >0 && distinct on (symbol) -> example case
					row_number() over (w_s order by dev_adjusted_close_perc_change_wom desc) 	as iserror_adjusted_close_perc_change_dev_wom_rn, 	-- agg >0 && rn=1 -> max case
					row_number() over (w_s order by dev_volume_dif desc) 				as iserror_volume_dif_dev_rn 				-- agg >0 && rn=1 -> max case
				from tickers_checks
				window w_s as (partition by symbol)
			),
		tickers_checks_verbose_union as
			(
				(
					select -- symbols that was used as market will give 0 here and will not trigger (until allowable sigma > 0)
						table_name||'__adjusted_close_perc_change_dev_wom__'||symbol as id,
						symbol, 
						table_name||'__adjusted_close_perc_change_dev_wom' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||table_name||' has '||iserror_adjusted_close_perc_change_dev_wom_agg||' rows with dev_adjusted_close_perc_change_wom exceeded allowed stddev='||allowable_sigma_dev_adjclose_percchange*stddev_adjusted_close_perc_change_wom||' from mu, while ticker''s (mu,stddev)=('||mean_adjusted_close_perc_change_wom||','||stddev_adjusted_close_perc_change_wom||'), max_dev='||dev_adjusted_close_perc_change_wom||', %of exceeding above stddev '||(dev_adjusted_close_perc_change_wom-stddev_adjusted_close_perc_change_wom)/(1e-30+stddev_adjusted_close_perc_change_wom)*100||'%. Example at '||dt as message,
						now() as updated_at
					from tickers_checks_agg
					where 	iserror_adjusted_close_perc_change_dev_wom_agg > 0
					and 	iserror_adjusted_close_perc_change_dev_wom_rn = 1 -- agg >0 && rn=1 -> max case
				)
				union all
				(
					select 
						table_name||'__volume_dif_dev__'||symbol as id,
						symbol, 
						table_name||'__volume_dif_dev' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||table_name||' has '||iserror_volume_dif_dev_agg||' rows with dev_volume_dif exceeded allowed stddev='||allowable_sigma_dev_volume_dif*stddev_volume_dif||' from mu, while ticker''s (mu,stddev)=('||mean_volume_dif||','||stddev_volume_dif||'), max_dev='||dev_volume_dif||', %of exceeding above stddev '||(dev_volume_dif-stddev_volume_dif)/(1e-30+stddev_volume_dif)*100||'%. Example at '||dt as message,
						now() as updated_at
					from tickers_checks_agg
					where 	iserror_volume_dif_dev_agg > 0
					and 	iserror_volume_dif_dev_rn = 1 -- agg >0 && rn=1 -> max case
				)
				union all
				(
					select distinct on (symbol) -- agg >0 && distinct on (symbol) -> example case
						table_name||'__adjusted_close_twice_same__'||symbol as id,
						symbol, 
						table_name||'__adjusted_close_twice_same' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||table_name||' has '||iserror_adjusted_close_twice_same_agg||' pairs of consecutive rows with same price. Example at '||dt as message,
						now() as updated_at
					from tickers_checks_agg 
					where iserror_adjusted_close_twice_same_agg > 0
				)
				union all
				(
					select distinct on (symbol) -- agg >0 && distinct on (symbol) -> example case
						table_name||'__adjusted_close_is_notpositive__'||symbol as id,
						symbol, 
						table_name||'__adjusted_close_is_notpositive' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||table_name||' has '||iserror_adjusted_close_is_notpositive_agg||' rows with adjusted_close not positive values (<=0). Example at '||dt as message,
						now() as updated_at
					from tickers_checks_agg 
					where iserror_adjusted_close_is_notpositive_agg > 0
				)
				union all
				(
					select distinct on (symbol) -- agg >0 && distinct on (symbol) -> example case
						table_name||'__adjusted_close_is_null__'||symbol as id,
						symbol, 
						table_name|| '__adjusted_close_is_null' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||table_name||' has '||iserror_adjusted_close_is_null_agg||' rows with adjusted_close = NULL values. Example at '||dt as message,
						now() as updated_at
					from tickers_checks_agg 
					where iserror_adjusted_close_is_null_agg > 0
				)
				union all
				(
					select distinct on (symbol) -- agg >0 && distinct on (symbol) -> example case
						table_name||'__volume_is_null__'||symbol as id,
						symbol, 
						table_name|| '__volume_is_null' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||table_name||' has '||iserror_volume_is_null_agg||' rows with volume = NULL values. Example at '||dt as message,
						now() as updated_at
					from tickers_checks_agg 
					where iserror_volume_is_null_agg > 0
				)
			)
		select
			id::varchar,
			symbol,
			code::varchar,
			"period"::varchar,
			message::varchar,
			updated_at
		from tickers_checks_verbose_union
	)

{% endif %}



{% if not var('realtime') %}

union all
	(
		with
		check_params(		table_name, 				dt_interval, 
					depth_stddev, 				depth_check, -- depth_stddev >= depth_check !!
					allowable_sigma_dev_adjclose_percchange, 	allowable_sigma_dev_volume_dif, 
					symbol_asmarket_crypto, 		symbol_asmarket_other) as
			(
				values 	('public_*.historical_prices', 	interval '1 day', 
					interval '5 year', 			interval '5 year', 
					5., 					10., 
					'BTC.CC', 				'SPY')
			),
		
		intrpl_dt_now_wrt_interval as -- generating time frames
			(	-- now()='2022-06-22 15:15:14' with dt_interval='15 minutes'::interval will give: '2022-06-22 15:15:00' (it drops last not full frame)
				select 	(to_timestamp(
							(extract(epoch from now()::timestamp)::int / extract(epoch from cp.dt_interval)::int
							) * extract(epoch from cp.dt_interval)::int
						) AT TIME ZONE 'UTC' --to cancel out TZ in to_timestamp(0)='1970-01-01 00:00:00+TZ' (will be shifted by 3 hrs if TZ was +3, ie '1970-01-01 03:00:00+03')
					)::timestamp as value
				from check_params cp
			),
		
		intrpl_symbol_asmarket_dt as 
			(
				select 	unnest(array[cp.symbol_asmarket_other, cp.symbol_asmarket_crypto]) as intrpl_symbol_asmarket,
					d.dt::timestamp
				from check_params cp
				left join intrpl_dt_now_wrt_interval on true
				cross join 
					generate_series(intrpl_dt_now_wrt_interval.value - cp.depth_stddev, 
							intrpl_dt_now_wrt_interval.value,
							cp.dt_interval) as d(dt)
			),
			
		/* 
		 * for cancel natural market price movements in measuring ticker anomaly price movements by "returns" (percentage price change d2d)
		 * we need to first subtract market returns from ticker returns (and only after that do some stat measurements by stdev of returns)
		 * so we need to guarantee that market(target) price will be presented in any dt that can occur in any ticker dt 
		 * and if we haven't row for market price when the ticker has - that mean we lost the market price data row 
		 * so we need to guarantee some reasonable intermediate value. The linear interpolation trick is all about it.
		 */
		intrpl_symbol_asmarket_dt_prices as 
			(
				select 	d.intrpl_symbol_asmarket,
					d.dt, 
					--t.adjusted_close as adjusted_close_raw, --have left it here if anyone wants to check
					coalesce(t.adjusted_close, 
						linear_interpolate(
							extract(epoch from d.dt),
							LAST_VALUE_IGNORENULLS(case when t.adjusted_close is not null then extract(epoch from d.dt) else null end) over (lookback),
							LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookback),
							LAST_VALUE_IGNORENULLS(case when t.adjusted_close is not null then extract(epoch from d.dt) else null end) over (lookforward),
							LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookforward)
							),
						LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookforward),
						LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookback),
						0) as intrpl_symbol_asmarket_adjusted_close
				from intrpl_symbol_asmarket_dt as d
				left join 
					(
						select 	code as intrpl_symbol_asmarket,
							"date"::timestamp as dt,
							adjusted_close
						from {{ ref('historical_prices') }} ehp
						join check_params cp on true
						where code = any(array[cp.symbol_asmarket_other, cp.symbol_asmarket_crypto])
					) as t using(intrpl_symbol_asmarket, dt)
				window
					lookback as (partition by d.intrpl_symbol_asmarket order by d.dt asc),
					lookforward as (partition by d.intrpl_symbol_asmarket order by d.dt desc)
			),
		
		tickers_data as
			(
				select 
					code 										as symbol, 
					to_timestamp("date",'YYYY-MM-DD')::timestamp 					as dt,
					adjusted_close,
					volume,
					case 	when t."type" = 'crypto' then cp.symbol_asmarket_crypto 
						else cp.symbol_asmarket_other end					as intrpl_symbol_asmarket
				from {{ ref('historical_prices') }} ehp
				join {{ ref('tickers') }} t on t.symbol = ehp.code -- we are interested in all tickers that are available in the app, so tickers table
				left join check_params cp on true 
				where to_timestamp("date",'YYYY-MM-DD')::timestamp >= now()::timestamp - cp.depth_stddev
			),
		tickers_lag as
			(
				select 
					t.*, -- symbol, dt, adjusted_close, volume, intrpl_symbol_asmarket
					lag(t.adjusted_close,1,t.adjusted_close) over (partition by t.symbol order by t.dt asc) 	as adjusted_close_pre,
					lag(t.volume,1,t.volume) over (partition by t.symbol order by t.dt asc) 			as volume_pre,
					sam.intrpl_symbol_asmarket_adjusted_close,
					lag(sam.intrpl_symbol_asmarket_adjusted_close,1,sam.intrpl_symbol_asmarket_adjusted_close) over (partition by t.symbol order by t.dt asc) 	as intrpl_symbol_asmarket_adjusted_close_pre
				from tickers_data t
				join intrpl_symbol_asmarket_dt_prices sam using (intrpl_symbol_asmarket, dt)
			),
		tickers_difs as
			(
				select
					*,
					adjusted_close - adjusted_close_pre 		as adjusted_close_dif,
					(adjusted_close - adjusted_close_pre)
						/(1e-30 + abs(adjusted_close_pre)) 	as adjusted_close_perc_change,
					volume - volume_pre 				as volume_dif,
					intrpl_symbol_asmarket_adjusted_close - intrpl_symbol_asmarket_adjusted_close_pre 	as intrpl_symbol_asmarket_adjusted_close_dif,
					(intrpl_symbol_asmarket_adjusted_close - intrpl_symbol_asmarket_adjusted_close_pre)
						/(1e-30 + abs(intrpl_symbol_asmarket_adjusted_close_pre))			as intrpl_symbol_asmarket_adjusted_close_perc_change,
					(adjusted_close - adjusted_close_pre)
						/(1e-30 + abs(adjusted_close_pre))
					- (intrpl_symbol_asmarket_adjusted_close - intrpl_symbol_asmarket_adjusted_close_pre)
						/(1e-30 + abs(intrpl_symbol_asmarket_adjusted_close_pre))			as adjusted_close_perc_change_wom
				from tickers_lag 	
			),
		tickers_stddevs_means_devs as
			(
				select 
					*,
					stddev_pop(adjusted_close_perc_change_wom) over (w_s) 	as stddev_adjusted_close_perc_change_wom,
					avg(adjusted_close_perc_change_wom) over (w_s) 		as mean_adjusted_close_perc_change_wom,
					abs(adjusted_close_perc_change_wom 
						- avg(adjusted_close_perc_change_wom) over (w_s)) 	as dev_adjusted_close_perc_change_wom,
					stddev_pop(volume_dif) over (w_s) 			as stddev_volume_dif,
					avg(volume_dif) over (w_s) 				as mean_volume_dif,
					abs(volume_dif 
						- avg(volume_dif) over (w_s)) 			as dev_volume_dif
				from tickers_difs
				window w_s as (partition by symbol)
			),
		tickers_checks as 
			(
				select 
					*,
					case when dev_adjusted_close_perc_change_wom > 
						stddev_adjusted_close_perc_change_wom * cp.allowable_sigma_dev_adjclose_percchange
												then 1 else 0 end 	as iserror_adjusted_close_perc_change_dev_wom,
					case when dev_volume_dif > 
						stddev_volume_dif * cp.allowable_sigma_dev_volume_dif
												then 1 else 0 end 	as iserror_volume_dif_dev,
					case when adjusted_close = adjusted_close_pre 		then 1 else 0 end 	as iserror_adjusted_close_twice_same,
					case when adjusted_close <= 0 				then 1 else 0 end 	as iserror_adjusted_close_is_notpositive,
					case when adjusted_close is null 			then 1 else 0 end 	as iserror_adjusted_close_is_null,
					case when volume is null 				then 1 else 0 end 	as iserror_volume_is_null
				from tickers_stddevs_means_devs
				left join check_params cp on true
				where dt >= now()::timestamp - cp.depth_check
			),
		tickers_checks_agg as -- I have left all the fields here, so someone could see them closer (copy and past all code up to here and see the output)
			(
				select 
					*, 
					sum(iserror_adjusted_close_perc_change_dev_wom) over (w_s) 	as iserror_adjusted_close_perc_change_dev_wom_agg, 	-- agg >0 && distinct on (symbol) -> example case
					sum(iserror_volume_dif_dev) over (w_s) 				as iserror_volume_dif_dev_agg, 				-- agg >0 && distinct on (symbol) -> example case
					sum(iserror_adjusted_close_twice_same) over (w_s) 		as iserror_adjusted_close_twice_same_agg, 		-- agg >0 && distinct on (symbol) -> example case
					sum(iserror_adjusted_close_is_notpositive) over (w_s) 		as iserror_adjusted_close_is_notpositive_agg, 		-- agg >0 && distinct on (symbol) -> example case
					sum(iserror_adjusted_close_is_null) over (w_s) 			as iserror_adjusted_close_is_null_agg, 			-- agg >0 && distinct on (symbol) -> example case
					sum(iserror_volume_is_null) over (w_s) 				as iserror_volume_is_null_agg, 				-- agg >0 && distinct on (symbol) -> example case
					row_number() over (w_s order by dev_adjusted_close_perc_change_wom desc) 	as iserror_adjusted_close_perc_change_dev_wom_rn, 	-- agg >0 && rn=1 -> max case
					row_number() over (w_s order by dev_volume_dif desc) 				as iserror_volume_dif_dev_rn 				-- agg >0 && rn=1 -> max case
				from tickers_checks
				window w_s as (partition by symbol)
			),
		tickers_checks_verbose_union as
			(
				(
					select -- symbols that was used as market will give 0 here and will not trigger (until allowable sigma > 0)
						table_name||'__adjusted_close_perc_change_dev_wom__'||symbol as id,
						symbol, 
						table_name||'__adjusted_close_perc_change_dev_wom' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||table_name||' has '||iserror_adjusted_close_perc_change_dev_wom_agg||' rows with dev_adjusted_close_perc_change_wom exceeded allowed stddev='||allowable_sigma_dev_adjclose_percchange*stddev_adjusted_close_perc_change_wom||' from mu, while ticker''s (mu,stddev)=('||mean_adjusted_close_perc_change_wom||','||stddev_adjusted_close_perc_change_wom||'), max_dev='||dev_adjusted_close_perc_change_wom||', %of exceeding above stddev '||(dev_adjusted_close_perc_change_wom-stddev_adjusted_close_perc_change_wom)/(1e-30+stddev_adjusted_close_perc_change_wom)*100||'%. Example at '||dt as message,
						now() as updated_at
					from tickers_checks_agg
					where 	iserror_adjusted_close_perc_change_dev_wom_agg > 0
					and 	iserror_adjusted_close_perc_change_dev_wom_rn = 1 -- agg >0 && rn=1 -> max case
				)
				union all
				(
					select 
						table_name||'__volume_dif_dev__'||symbol as id,
						symbol, 
						table_name||'__volume_dif_dev' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||table_name||' has '||iserror_volume_dif_dev_agg||' rows with dev_volume_dif exceeded allowed stddev='||allowable_sigma_dev_volume_dif*stddev_volume_dif||' from mu, while ticker''s (mu,stddev)=('||mean_volume_dif||','||stddev_volume_dif||'), max_dev='||dev_volume_dif||', %of exceeding above stddev '||(dev_volume_dif-stddev_volume_dif)/(1e-30+stddev_volume_dif)*100||'%. Example at '||dt as message,
						now() as updated_at
					from tickers_checks_agg
					where 	iserror_volume_dif_dev_agg > 0
					and 	iserror_volume_dif_dev_rn = 1 -- agg >0 && rn=1 -> max case
				)
				union all
				(
					select distinct on (symbol) -- agg >0 && distinct on (symbol) -> example case
						table_name||'__adjusted_close_twice_same__'||symbol as id,
						symbol, 
						table_name||'__adjusted_close_twice_same' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||table_name||' has '||iserror_adjusted_close_twice_same_agg||' pairs of consecutive rows with same price. Example at '||dt as message,
						now() as updated_at
					from tickers_checks_agg 
					where iserror_adjusted_close_twice_same_agg > 0
				)
				union all
				(
					select distinct on (symbol) -- agg >0 && distinct on (symbol) -> example case
						table_name||'__adjusted_close_is_notpositive__'||symbol as id,
						symbol, 
						table_name||'__adjusted_close_is_notpositive' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||table_name||' has '||iserror_adjusted_close_is_notpositive_agg||' rows with adjusted_close not positive values (<=0). Example at '||dt as message,
						now() as updated_at
					from tickers_checks_agg 
					where iserror_adjusted_close_is_notpositive_agg > 0
				)
				union all
				(
					select distinct on (symbol) -- agg >0 && distinct on (symbol) -> example case
						table_name||'__adjusted_close_is_null__'||symbol as id,
						symbol, 
						table_name|| '__adjusted_close_is_null' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||table_name||' has '||iserror_adjusted_close_is_null_agg||' rows with adjusted_close = NULL values. Example at '||dt as message,
						now() as updated_at
					from tickers_checks_agg 
					where iserror_adjusted_close_is_null_agg > 0
				)
				union all
				(
					select distinct on (symbol) -- agg >0 && distinct on (symbol) -> example case
						table_name||'__volume_is_null__'||symbol as id,
						symbol, 
						table_name|| '__volume_is_null' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||table_name||' has '||iserror_volume_is_null_agg||' rows with volume = NULL values. Example at '||dt as message,
						now() as updated_at
					from tickers_checks_agg 
					where iserror_volume_is_null_agg > 0
				)
			)
		select
			id::varchar,
			symbol,
			code::varchar,
			"period"::varchar,
			message::varchar,
			updated_at
		from tickers_checks_verbose_union
	)

{% endif %}
