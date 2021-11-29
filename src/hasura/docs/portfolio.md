### Check whether profile has linked plaid accounts

```graphql
{
    app_profiles {
        profile_plaid_access_tokens {
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
    link_plaid_account(profile_id: 2, public_token: "public-sandbox-0c02e9cb-ef57-4a82-a4eb-f8d7d99dfb01") {
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

### Get Holdings

```graphql
{
    get_portfolio_holdings(profile_id: 2) {
        id
        quantity
        iso_currency_code
        profile_id
        security_id
        security {
            close_price
            close_price_as_of
            created_at
            iso_currency_code
            id
            name
            ticker_symbol
            type
            updated_at
            tickers {
                name
                sector
                symbol
            }
        }
        account {
            balance_available
            balance_current
            balance_iso_currency_code
            balance_limit
            created_at
            id
            mask
            name
            official_name
            subtype
            type
        }
    }
}
```

### Get Transactions

```graphql
{
    get_portfolio_transactions(profile_id: 1) {
        id
        quantity
        iso_currency_code
        profile_id
        security_id
        security {
            close_price
            close_price_as_of
            created_at
            iso_currency_code
            id
            name
            ticker_symbol
            type
            updated_at
            tickers {
                name
                sector
                symbol
            }
        }
        account {
            balance_available
            balance_current
            balance_iso_currency_code
            balance_limit
            created_at
            id
            mask
            name
            official_name
            subtype
            type
        }
        amount
        date
        fees
        name
        price
    }
}
```

### Gains

```GraphQL
{
  app_profiles {
    portfolio_gains {
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
    profile_holdings {
      portfolio_holding_gains {
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
        value_to_portfolio_value
        ltt_quantity_total
      }
      holding_transactions {
        portfolio_transaction_gains {
          absolute_gain_1d
          absolute_gain_1m
          absolute_gain_1w
          absolute_gain_1y
          absolute_gain_3m
          absolute_gain_5y
          absolute_gain_total
          relative_gain_1d
          relative_gain_1m
          relative_gain_1w
          relative_gain_1y
          relative_gain_3m
          relative_gain_5y
          relative_gain_total
        }
      }
    }
  }
}
```

### Chart

periods: 15min, 1d, 1w, 1m

```GraphQL
{
    portfolio_chart(where: {profile_id: {_eq: 1}, period: {_eq: "1d"}}, order_by: {date: asc}) {
        datetime
        period
        value
    }
}
```

### Sorting / Filtering

```GraphQL
{
    # app_profile_holdings(order_by: {holding_details: {purchase_date: asc}})
    # app_profile_holdings(order_by: {holding_details: {relative_gain_total: asc}})
    # app_profile_holdings(order_by: {holding_details: {relative_gain_1d: asc}})
    # app_profile_holdings(order_by: {holding_details: {value_to_portfolio_value: asc}})
    # app_profile_holdings(order_by: {holding_details: {ticker_name: asc}})
    # app_profile_holdings(order_by: {holding_details: {market_capitalization: asc}})
    # app_profile_holdings(order_by: {holding_details: {next_earnings_date: asc}})
    # app_profile_holdings(where: {holding_details: {account_id: {_eq: 7}}})
    # app_profile_holdings(where: {holding_details: {ticker: {ticker_interests: {interest_id: {_in: [5]}}}}})
    # app_profile_holdings(where: {holding_details: {ticker: {ticker_categories: {category_id: {_in: []}}}}}) 
    # app_profile_holdings(where: {holding_details: {security_type: {_in: ["equity"]}}})
    # app_profile_holdings(where: {holding_details: {ltt_quantity_total: {_gt: 0}}})
    
    # account options:
    app_profile_portfolio_accounts {
        balance_available
        balance_current
        balance_iso_currency_code
        balance_limit
        created_at
        mask
        name
        official_name
        subtype
        type
        updated_at
    }
    
    # interests options:
    interests(where: {enabled: {_eq: "1"}}) {
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
}
```
