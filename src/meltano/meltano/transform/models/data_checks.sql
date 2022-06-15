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
		check_params(table_name, depth_stddev_days, depth_check_days, allowable_sigma_dev_adjclose_percchange, allowable_sigma_dev_volume_dif) as
			(
				values ('eod_historical_prices', 250, 7*2, 3., 3.)
			),
		tickers_lag as
			(
				select 
					code 							as symbol, 
					to_date("date",'YYYY-MM-DD') 	as date_cur,
					adjusted_close 					as adjusted_close_cur,
					volume 							as volume_cur,
					lag("date",1,"date") over (partition by code order by "date") as date_pre,
					lag(adjusted_close,1,adjusted_close) over (partition by code order by "date") as adjusted_close_pre,
					lag(volume,1,volume) over (partition by code order by "date") as volume_pre
				from {{ source('eod', 'eod_historical_prices') }} ehp
				join {{ ref('tickers') }} t on t.symbol = ehp.code
				where to_date("date",'YYYY-MM-DD') >= now() - make_interval(0,0,0,(select depth_stddev_days from check_params))
			),
		tickers_difs as
			(
				select 
					*,
					adjusted_close_cur - adjusted_close_pre 	as adjusted_close_dif,
					(adjusted_close_cur - adjusted_close_pre)
						/(1e-30 + adjusted_close_pre) 			as adjusted_close_perc_change,
					volume_cur - volume_pre 					as volume_dif
				from tickers_lag
			),
		tickers_stddevs_means_devs as
			(
				select 
					*,
					stddev_pop(adjusted_close_perc_change) over (partition by symbol) 	as stddev_adjusted_close_perc_change,
					avg(adjusted_close_perc_change) over (partition by symbol) 			as mean_adjusted_close_perc_change,
					abs(adjusted_close_perc_change 
						- avg(adjusted_close_perc_change) over (partition by symbol)) 	as dev_adjusted_close_perc_change,
					stddev_pop(volume_dif) over (partition by symbol) 					as stddev_volume_dif,
					avg(volume_dif) over (partition by symbol) 							as mean_volume_dif,
					abs(volume_dif 
						- avg(volume_dif) over (partition by symbol)) 					as dev_volume_dif
				from tickers_difs
			),
		tickers_checks as -- I have left all the source fields here, so someone could see them closer by check this cte
			(
				select 
					*,
					case when dev_adjusted_close_perc_change > 
						stddev_adjusted_close_perc_change * (select allowable_sigma_dev_adjclose_percchange from check_params)
							then 1 else 0 end 																			as iserror_adjusted_close_perc_change_dev,
					case when dev_volume_dif > 
						stddev_volume_dif * (select allowable_sigma_dev_volume_dif from check_params)
							then 1 else 0 end 																			as iserror_volume_dif_dev,
					case when adjusted_close_cur = adjusted_close_pre then 1 else 0 end 								as iserror_adjusted_close_twice_same,
					case when volume_cur = 0 and volume_pre = 0 then 1 else 0 end 										as iserror_volume_twice_zero,
					case when adjusted_close_cur is null then 1 else 0 end 												as iserror_adjusted_close_is_null,
					case when volume_cur is null then 1 else 0 end 														as iserror_volume_is_null
				from tickers_stddevs_means_devs
				where date_cur >= now() - make_interval(0,0,0,(select depth_check_days from check_params))
			),
		tickers_checks_union as
			(
				with
				q_agg as
					(
						select
							symbol,
							mean_adjusted_close_perc_change,
							stddev_adjusted_close_perc_change,
							max(dev_adjusted_close_perc_change) 			as max_dev_adjusted_close_perc_change,
							mean_volume_dif,
							stddev_volume_dif,
							max(dev_volume_dif) 							as max_dev_volume_dif,
							sum(iserror_adjusted_close_perc_change_dev) 	as iserror_adjusted_close_perc_change_dev,
							sum(iserror_volume_dif_dev) 					as iserror_volume_dif_dev,
							sum(iserror_adjusted_close_twice_same) 			as iserror_adjusted_close_twice_same,
							sum(iserror_volume_twice_zero) 					as iserror_volume_twice_zero,
							sum(iserror_adjusted_close_is_null) 			as iserror_adjusted_close_is_null,
							sum(iserror_volume_is_null) 					as iserror_volume_is_null
						from tickers_checks
						group by symbol, 
							mean_adjusted_close_perc_change, stddev_adjusted_close_perc_change, mean_volume_dif, stddev_volume_dif
					)
				(
					select ((select table_name from check_params)||'_adjusted_close_perc_change_dev' || '_' || symbol)::varchar as id,
						symbol, 
						(select table_name from check_params)||'_adjusted_close_perc_change_dev' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||(select table_name from check_params)||' has '||iserror_adjusted_close_perc_change_dev||' rows with adjusted_close_perc_change_dev exceeded allowed stddev='||(select allowable_sigma_dev_adjclose_percchange from check_params)*stddev_adjusted_close_perc_change||' from mu, while ticker''s (mu,stddev)=('||mean_adjusted_close_perc_change||','||stddev_adjusted_close_perc_change||'), max_dev='||max_dev_adjusted_close_perc_change||', %of exceeding above stddev '||(max_dev_adjusted_close_perc_change-stddev_adjusted_close_perc_change)/(1e-30+stddev_adjusted_close_perc_change)*100||'%' as message,
						now() as updated_at
					from q_agg where iserror_adjusted_close_perc_change_dev > 0
				)
				union
				(
					select ((select table_name from check_params)||'_volume_dif_dev' || '_' || symbol)::varchar as id,
						symbol, 
						(select table_name from check_params)||'_volume_dif_dev' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||(select table_name from check_params)||' has '||iserror_volume_dif_dev||' rows with volume_dif_dev exceeded allowed stddev='||(select allowable_sigma_dev_volume_dif from check_params)*stddev_volume_dif||' from mu, while ticker''s (mu,stddev)=('||mean_volume_dif||','||stddev_volume_dif||'), max_dev='||max_dev_volume_dif||', %of exceeding above stddev '||(max_dev_volume_dif-stddev_volume_dif)/(1e-30+stddev_volume_dif)*100||'%' as message,
						now() as updated_at
					from q_agg where iserror_volume_dif_dev > 0
				)
				union
				(
					select ((select table_name from check_params)||'_adjusted_close_twice_same' || '_' || symbol)::varchar as id,
						symbol, 
						(select table_name from check_params)||'_adjusted_close_twice_same' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||(select table_name from check_params)||' has '||iserror_adjusted_close_twice_same||' pairs of consecutive rows with same price'  as message,
						now() as updated_at
					from q_agg where iserror_adjusted_close_twice_same > 0
				)
				union
				(
					select ((select table_name from check_params)||'_volume_twice_zero' || '_' || symbol)::varchar as id,
						symbol, 
						(select table_name from check_params)||'_volume_twice_zero' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||(select table_name from check_params)||' has '||iserror_volume_twice_zero||' pairs of consecutive rows with 0 volume'  as message,
						now() as updated_at
					from q_agg where iserror_volume_twice_zero > 0
				)
				union
				(
					select ((select table_name from check_params)||'_adjusted_close_is_null' || '_' || symbol)::varchar as id,
						symbol, 
						(select table_name from check_params)||'_adjusted_close_is_null' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||(select table_name from check_params)||' has '||iserror_adjusted_close_is_null||' rows with NULL adjusted_close'  as message,
						now() as updated_at
					from q_agg where iserror_adjusted_close_is_null > 0
				)
				union
				(
					select ((select table_name from check_params)||'_volume_is_null' || '_' || symbol)::varchar as id,
						symbol, 
						(select table_name from check_params)||'_volume_is_null' as code,
						'daily' as "period",
						'Tickers '||symbol||' in table '||(select table_name from check_params)||' has '||iserror_volume_is_null||' rows with NULL volume'  as message,
						now() as updated_at
					from q_agg where iserror_volume_is_null > 0
				)
				
			)
		select * from tickers_checks_union
  )

{% endif %}
