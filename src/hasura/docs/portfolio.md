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
