alter table "app"."profile_ticker_metrics_settings" drop constraint "field_name";
alter table "app"."profile_ticker_metrics_settings" add constraint "field_name" check (field_name::text = ANY (ARRAY['avg_volume_10d'::character varying, 'short_percent_outstanding'::character varying, 'shares_outstanding'::character varying, 'avg_volume_90d'::character varying, 'shares_float'::character varying, 'short_ratio'::character varying, 'beta'::character varying, 'absolute_historical_volatility_adjusted_current'::character varying, 'relative_historical_volatility_adjusted_current'::character varying, 'absolute_historical_volatility_adjusted_min_1y'::character varying, 'absolute_historical_volatility_adjusted_max_1y'::character varying, 'relative_historical_volatility_adjusted_min_1y'::character varying, 'relative_historical_volatility_adjusted_max_1y'::character varying, 'implied_volatility'::character varying, 'revenue_growth_yoy'::character varying, 'revenue_growth_fwd'::character varying, 'ebitda_growth_yoy'::character varying, 'eps_growth_yoy'::character varying, 'eps_growth_fwd'::character varying, 'address_city'::character varying, 'address_state'::character varying, 'address_county'::character varying, 'address_full'::character varying, 'exchange_name'::character varying, 'market_capitalization'::character varying, 'enterprise_value_to_sales'::character varying, 'price_to_earnings_ttm'::character varying, 'price_to_sales_ttm'::character varying, 'price_to_book_value'::character varying, 'enterprise_value_to_ebitda'::character varying, 'price_change_1m'::character varying, 'price_change_3m'::character varying, 'price_change_1y'::character varying, 'dividend_yield'::character varying, 'dividends_per_share'::character varying, 'dividend_payout_ratio'::character varying, 'years_of_consecutive_dividend_growth'::character varying, 'dividend_frequency'::character varying, 'eps_actual'::character varying, 'eps_estimate'::character varying, 'beaten_quarterly_eps_estimation_count_ttm'::character varying, 'eps_surprise'::character varying, 'revenue_estimate_avg_0y'::character varying, 'revenue_actual'::character varying, 'profit_margin'::character varying, 'revenue_ttm'::character varying, 'revenue_per_share_ttm'::character varying, 'net_income'::character varying, 'roi'::character varying, 'asset_cash_and_equivalents'::character varying, 'roa'::character varying, 'total_assets'::character varying, 'ebitda'::character varying, 'net_debt'::character varying]::text[]));
