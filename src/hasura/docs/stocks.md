### Alternative stocks

```graphql
{
    tickers {
        symbol
        ticker_industries {
            industry_order
            gainy_industry {
                name
                ticker_industries {
                    ticker { # here are our alternative stocks
                        symbol
                    }
                }
            }
        }
        ticker_interests {
            symbol
            interest_id
            interest {
                icon_url
                id
                name
            }
        }
        ticker_analyst_ratings {
            strong_buy
            buy
            hold
            sell
            strong_sell
            rating
        }
    }
}
```

### Ticker metrics

```graphql
{
    tickers(order_by: {ticker_metrics: {month_price_change: asc}}) {
        symbol
        name
        description
        ticker_metrics {
            # trading
            avg_volume_10d
            short_percent
            shares_outstanding
            avg_volume_90d
            shares_float
            short_ratio
            beta
            implied_volatility
            # growth
            revenue_growth_yoy
            revenue_growth_fwd
            ebitda_growth_yoy
            eps_growth_yoy
            eps_growth_fwd
            #general
            address_city
            address_state
            address_county
            address_full
            exchange_name
            #valuation
            market_capitalization
            enterprise_value_to_sales
            price_to_earnings_ttm
            price_to_sales_ttm
            price_to_book_value
            enterprise_value_to_ebitda
            #momentum
            price_change_1m
            price_change_3m
            price_change_1y
            #dividend
            dividend_yield
            dividends_per_share
            dividend_payout_ratio
            years_of_consecutive_dividend_growth
            dividend_frequency
            #earnings
            eps_ttm
            eps_estimate
            beaten_quarterly_eps_estimation_count_ttm
            eps_difference
            revenue_estimate_avg_0y
            revenue_actual
            #financials
            profit_margin
            revenue_ttm
            revenue_per_share_ttm
            net_income_ttm
            roi
            asset_cash_and_equivalents
            roa
            total_assets
            ebitda_ttm
            net_debt
            
            prev_price_1d
            prev_price_1w
            prev_price_1m
            prev_price_3m
            prev_price_1y
            prev_price_5y
            prev_price_all
        }
        ticker_industries {
            industry_order
            gainy_industry {
                id
                name
            }
        }
    }
}
```

### Ticker search

```graphql
query  {
    search_tickers(query: "momentum", limit: 5) {
        symbol
        ticker {
            name
            description
            ticker_highlights {
                highlight
            }
        }
    }
}
```

### Get ticker latest trading session (for the "market has just opened" disclaimer)
```graphql
query GetTickerLatestTradingSession($symbol: String!) {
  ticker_latest_trading_session(where: {symbol: {_eq: $symbol}}) {
    date
    open_at
  }
}
```
