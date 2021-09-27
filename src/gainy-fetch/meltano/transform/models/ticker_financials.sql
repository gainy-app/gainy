{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}


with prices_tmp as (
    select
        hp.code,
        hp."date",
        hp.price,
        hp.cur_date,
        hp.cur_price,
        case
            when hp."date" <= hp.cur_date - interval '3 month' then '3m'
            when hp."date" <= hp.cur_date - interval '1 month' then '1m'
        end as year_flag
	from (
		select
			code,
			"date"::date,
			adjusted_close::numeric as price,
			first_value("date"::date) over (partition by code order by "date" desc) as cur_date,
			first_value(adjusted_close::numeric) over (partition by code order by "date" desc) as cur_price
		from
			public.historical_prices
	) hp
),
prices_ago as (
	select
		code,
		max(cur_price) as cur_price,
		max(price) filter (where year_flag = '1m') as price_1m_ago,
		max(price) filter (where year_flag = '3m') as price_3m_ago
	from (
		select distinct on (code, year_flag)
			code, year_flag, price, cur_price
		from prices_tmp
		order by code, year_flag, "date" desc
		) p
	group by code
),
dividends_tmp as (
	select
        d.code,
        d."date",
        d.value,
        d.latest_date,
        d.latest_value,
        case
            when d."date" <= d.latest_date - interval '5 year' then '5y'
            when d."date" <= d.latest_date - interval '1 year' then '1y'
        end as year_flag
	from (
		select
			code,
			"date"::date,
			value::numeric as value,
			first_value("date"::date) over (partition by code order by "date" desc) as latest_date,
			first_value("value"::numeric) over (partition by code order by "date" desc) as latest_value
		from
			public.dividends
	) d
),
dividends_ago as (
	select
		code,
		max(latest_value) as latest_value,
		max(value) filter (where year_flag = '1y') as value_1y_ago,
		max(value) filter (where year_flag = '5y') as value_5y_ago
	from (
		select distinct on (code, year_flag)
			code, year_flag, value, latest_value
		from dividends_tmp
		order by code, year_flag, "date" desc
		) p
	group by code
),
    latest_highlight as (
    select distinct on (symbol) *
    from {{ ref ('ticker_highlights') }}
    order by symbol
    )
select  DISTINCT ON (h.symbol) h.symbol,
        pe_ratio,
        market_capitalization,
        highlight,
        h.revenue_ttm,
        CASE
            WHEN da.latest_value = 0.0 THEN NULL
            WHEN da.value_5y_ago IS NOT NULL THEN pow(da.latest_value / da.value_5y_ago, 1.0 / 5)::real - 1
            WHEN da.value_1y_ago IS NOT NULL THEN (da.latest_value / da.value_1y_ago)::real - 1
        END                                  as dividend_growth,
        pa.price_3m_ago::real                as quarter_ago_close,
        case
            when price_1m_ago != 0 then (cur_price / price_3m_ago - 1)::real
        end                                  as quarter_price_performance,
        now()                                as created_at,
        h.profit_margin::real                as net_profit_margin,
        v.enterprise_value_revenue::real     as enterprise_value_to_sales,
        h.quarterly_revenue_growth_yoy::real as quarterly_revenue_growth_yoy,
        case
            when price_1m_ago != 0 then (cur_price / price_1m_ago - 1)::real
        end                                  as month_price_performance
from {{ ref('highlights') }} h
    left join {{ ref('valuation') }} v on h.symbol = v.symbol
    left join latest_highlight th on h.symbol = th.symbol
    left join prices_ago pa on h.symbol = pa.code
    left join dividends_ago da on h.symbol = da.code
order by h.symbol
