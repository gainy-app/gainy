### Check whether profile has linked plaid accounts

```graphql
{
    app_profiles {
        profile_plaid_access_tokens(where: {purpose: {_eq: "portfolio"}}) {
            id
        }
    }
}
```

### Create Link Token

```graphql
{
    create_plaid_link_token(profile_id: 2, redirect_uri: "https://app.gainy.application.ios"){
        link_token
    }
}
```

### Link Account

```graphql
{
    link_plaid_account(profile_id: 2, 
        public_token: "public-sandbox-0c02e9cb-ef57-4a82-a4eb-f8d7d99dfb01") {
        result
        plaid_access_token_id
    }
}
```

### Reauth flow

The app needs to launch the reauth flow when the access token's `needs_reauth_since` field is not null:
```graphql
{
  app_profile_plaid_access_tokens(where: {purpose: {_eq: "portfolio"}}) { needs_reauth_since }
}
```

Then you need to pass `access_token_id` along with other fields to the `create_plaid_link_token` and `link_plaid_account` endpoints: 
```graphql
{
    create_plaid_link_token(
        profile_id: 2, 
        redirect_uri: "https://app.gainy.application.ios",
        access_token_id: 1
    ){
        link_token
    }
}
```
```graphql
{
    link_plaid_account(
        profile_id: 2,
        public_token: "public-sandbox-0c02e9cb-ef57-4a82-a4eb-f8d7d99dfb01",
        access_token_id: 1
    ) {
        result
    }
}
```

### Unlink Account

```graphql
mutation{
    delete_app_profile_plaid_access_tokens_by_pk(id: 1){
        id
    }
}
```

### Chart

periods: 1d, 1w, 1m, 3m, 1y, 5y, all

```GraphQL
query GetPortfolioChart(
    $profileId: Int!,
    $periods: [String]!,
    $interestIds: [Int],
    $categoryIds: [Int],
    $lttOnly: Boolean,
    $securityTypes: [String]
) {
    get_portfolio_chart(
        profile_id: $profileId,
        periods: $periods,
        interest_ids: $interestIds,
        category_ids: $categoryIds,
        ltt_only: $lttOnly,
        security_types: $securityTypes
    ) {
        datetime
        period
        open
        high
        low
        close
    }
    get_portfolio_chart_previous_period_close(
        profile_id: $profileId,
        interest_ids: $interestIds,
        category_ids: $categoryIds,
        ltt_only: $lttOnly,
        security_types: $securityTypes
    ) {
        prev_close_1d
        prev_close_1w
        prev_close_1m
        prev_close_3m
        prev_close_1y
        prev_close_5y
    }
}
```

### Pie Chart

entity_type = ticker | category | interest | security_type

```graphql
query GetPortfolioPieChart(
    $profileId: Int!,
    $accessTokenIds: [Int]
) {
    get_portfolio_piechart(
        profile_id: $profileId,
        access_token_ids: $accessTokenIds
    ) {
        weight
        entity_type
        relative_daily_change
        entity_name
        entity_id
        absolute_value
        absolute_daily_change
    }
}
```

### Sorting / Filtering

```GraphQL
{
    # app_profile_holdings(where: {holding_details: {ticker: {ticker_interests: {interest_id: {_in: [5]}}}}})
    # app_profile_holdings(where: {holding_details: {ticker: {ticker_categories: {category_id: {_in: []}}}}}) 
    # app_profile_holdings(where: {holding_details: {security_type: {_in: ["equity"]}}})
    # app_profile_holdings(where: {holding_details: {ltt_quantity_total: {_gt: 0}}})
    
    # broker options:
    app_profile_plaid_access_tokens(where: {purpose: {_eq: "portfolio"}}) { id }

    # interests options:
    interests(where: {enabled: {_eq: "1"}}, order_by: {sort_order: asc}) {
        id
        name
        icon_url
    }
    
    # categories options:
    categories {
        id
        name
        icon_url
    }

    # security_type options:
    #    cash: Cash, currency, and money market funds
    #    derivative: Options, warrants, and other derivative instruments
    #    equity: Domestic and foreign equities
    #    etf: Multi-asset exchange-traded investment funds
    #    fixed income: Bonds and certificates of deposit (CDs)
    #    loan: Loans and loan receivables.
    #    mutual fund: Open- and closed-end vehicles pooling funds of multiple investors.
    #    other: Unknown or other investment types
    #    ttf: TTF
}
```

### Full request with grouping

```GraphQL
query GetPlaidHoldings($profileId: Int!) {
    portfolio_gains (where:{profile_id: {_eq: $profileId}}) {
        absolute_gain_1d
        absolute_gain_1m
        absolute_gain_1w
        absolute_gain_1y
        absolute_gain_3m
        absolute_gain_5y
        absolute_gain_total
        actual_value
        relative_gain_1d
        relative_gain_1m
        relative_gain_1w
        relative_gain_1y
        relative_gain_3m
        relative_gain_5y
        relative_gain_total
    }
    profile_holding_groups(where:{profile_id: {_eq: $profileId}}) {
        tags(order_by: {priority: desc}) {
            collection{
                id
                name
            }
        }
        details {
            ltt_quantity_total
            market_capitalization
            next_earnings_date
            purchase_date
            name
            ticker_symbol
            collection_id
            ticker_symbol
            relative_gain_1d
            relative_gain_total
        }
        gains{
            absolute_gain_1d
            absolute_gain_1m
            absolute_gain_1w
            absolute_gain_1y
            absolute_gain_3m
            absolute_gain_5y
            absolute_gain_total
            actual_value
            ltt_quantity_total
            relative_gain_1m
            relative_gain_1d
            relative_gain_1w
            relative_gain_1y
            relative_gain_3m
            relative_gain_5y
            relative_gain_total
            value_to_portfolio_value
        }
        holdings {
            name
            quantity
            ticker_symbol
            type
            broker {
                uniq_id
                name
            }
            holding_details {
                purchase_date
                ticker_symbol
                ticker_name
                market_capitalization
                next_earnings_date
                ltt_quantity_total
                security_type
                relative_gain_1d
                relative_gain_total
            }
            gains {
                absolute_gain_1d
                absolute_gain_1m
                absolute_gain_1w
                absolute_gain_1y
                absolute_gain_3m
                absolute_gain_5y
                absolute_gain_total
                actual_value
                ltt_quantity_total
                relative_gain_1m
                relative_gain_1d
                relative_gain_1w
                relative_gain_1y
                relative_gain_3m
                relative_gain_5y
                relative_gain_total
                value_to_portfolio_value
            }
        }
        symbol

        ticker {
            name
            ...RemoteTickerDetailsFull
            realtime_metrics {
                absolute_daily_change
                actual_price
                daily_volume
                relative_daily_change
                symbol
                time
            }
        }
    }
}
```

