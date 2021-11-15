### Alternative stocks

```graphql
{
    tickers {
        symbol
        ticker_industries {
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
            market_capitalization
            enterprise_value_to_sales
            month_price_change
            quarterly_revenue_growth_yoy
            profit_margin
            # trading
            avg_volume_10d
            short_percent_outstanding
            shares_outstanding
            avg_volume_90d
            shares_float
            short_ratio
            beta
            absolute_historical_volatility_adjusted_current
            relative_historical_volatility_adjusted_current
            absolute_historical_volatility_adjusted_min_1y
            absolute_historical_volatility_adjusted_max_1y
            relative_historical_volatility_adjusted_min_1y
            relative_historical_volatility_adjusted_max_1y
            implied_volatility
            # growth
            revenue_growth_yoy
            revenue_growth_fwd
            ebitda_growth_yoy
            eps_growth_yoy
            eps_growth_fwd
        }
        ticker_industries {
            gainy_industry {
                id
                name
            }
        }
    }
}
```