{{
  config(
    materialized = "table",
    sort = "created_at",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}


with prices_tmp as (
    select
        hp1.code,
        hp1."date",
        hp1.price,
        hp1.cur_date,
        hp2.price as cur_price,
        case
            when hp1."date" <= hp1.cur_date - interval '5 year' then '5y'
            when hp1."date" <= hp1.cur_date - interval '3 year' then '3y'
            when hp1."date" <= hp1.cur_date - interval '1 year' then '1y'
        end as years_flag
	from (
		select
			code,
			"date"::date,
			adjusted_close::numeric as price,
			max("date"::date) over (partition by code) as cur_date
		from
			public.historical_prices
	) hp1
	join (
		select
			code,
			"date"::date,
			adjusted_close::numeric as price
		from
			public.historical_prices
	) hp2
	on
		hp1.code = hp2.code and hp1.cur_date = hp2."date"::date
),
cagr_tmp as (
	select
		code,
		max(CAGR("date", price, cur_date, cur_price)) filter (where years_flag = '1y' and row_num = 1) as market_cap_cagr_1years,
		max(CAGR("date", price, cur_date, cur_price)) filter (where years_flag = '3y' and row_num = 1) as market_cap_cagr_3years,
		max(CAGR("date", price, cur_date, cur_price)) filter (where years_flag = '5y' and row_num = 1) as market_cap_cagr_5years
	from (
		select
			code,
			"date",
			price,
			cur_date,
			cur_price,
			years_flag,
			row_number() over (partition by code, years_flag order by "date" desc) as row_num
		from
			prices_tmp
		where
			years_flag is not null
	) p
	group by code
),
sma_tmp as (
	select
		code,
	   	avg("price") filter (where "date" > cur_date - interval '30 day') as sma_30days,
	   	avg("price") filter (where "date" > cur_date - interval '50 day') as sma_50days,
	   	avg("price") filter (where "date" > cur_date - interval '200 day') as sma_200days
	from
		prices_tmp
	group by
		code
)
select  h.symbol,
        pe_ratio,
        market_capitalization,
        highlight,
        h.revenue_ttm,
        CASE
            WHEN d0.value = 0 THEN NULL
            WHEN d1.value IS NOT NULL THEN pow(d0.value / d1.value, 1.0 / 5)::real - 1
            WHEN d2.value IS NOT NULL THEN (d0.value / d2.value)::real - 1
        END                   as dividend_growth,
        hp1.close                 as quarter_ago_close,
        hp0.close / hp1.close - 1 as quarter_price_performance,
        now()                      as created_at,
        h.profit_margin            as net_profit_margin,
        v.enterprise_value_revenue::real as enterprise_value_to_sales,
        sma_30days::real,
        sma_50days::real,
        sma_200days::real,
        market_cap_cagr_1years::real,
        market_cap_cagr_3years::real,
        market_cap_cagr_5years::real
from {{ ref('highlights') }} h
    left join sma_tmp on h.symbol = sma_tmp.code
    left join cagr_tmp on h.symbol = cagr_tmp.code
    left join {{ ref('valuation') }} v on h.symbol = v.symbol

    LEFT JOIN {{ ref ('ticker_highlights') }} th on h.symbol = th.symbol

    LEFT JOIN dividends d0 on h.symbol = d0.code
    LEFT JOIN dividends d0_next on h.symbol = d0_next.code AND d0_next.date:: timestamp > d0.date:: timestamp
    LEFT JOIN dividends d1 on h.symbol = d1.code AND d1.date:: timestamp < d0.date:: timestamp - interval '5 years'
    LEFT JOIN dividends d1_next on h.symbol = d1_next.code AND d1_next.date:: timestamp < d0.date:: timestamp - interval '5 years' AND d1_next.date:: timestamp > d1.date:: timestamp
    LEFT JOIN dividends d2 on h.symbol = d2.code AND d2.date:: timestamp < d0.date:: timestamp - interval '1 years'
    LEFT JOIN dividends d2_next on h.symbol = d2_next.code AND d2_next.date:: timestamp < d0.date:: timestamp - interval '1 years' AND d2_next.date:: timestamp > d2.date:: timestamp

    LEFT JOIN historical_prices hp0 on h.symbol = hp0.code
    LEFT JOIN historical_prices hp0_next on h.symbol = hp0_next.code AND hp0_next.date::timestamp > hp0.date::timestamp
    LEFT JOIN historical_prices hp1 on h.symbol = hp1.code AND hp1.date::timestamp < NOW() - interval '3 months'
    LEFT JOIN historical_prices hp1_next on h.symbol = hp1_next.code AND hp1_next.date::timestamp < NOW() - interval '3 months' AND hp1_next.date::timestamp > hp1.date::timestamp

WHERE d0_next.code is null
  AND d1_next.code is null
  AND d2_next.code is null
  AND hp0_next.code is null
  AND hp1_next.code is null
