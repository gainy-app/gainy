# Trading / Connecting bank account with plaid

1. [Generate link token](#generate-link-token)
2. [Exchange public_token and list accounts](#exchange-public_token-and-list-accounts)
3. [Link new funding account](#link-new-funding-account)
4. [List connected funding accounts](#list-connected-funding-accounts)
5. Portfolio tab must only work with plaid tokens `(where: {purpose: {_eq: "portfolio"}})`.
   See updated portfolio [requests](../portfolio.md).
6. [Delete connected funding account](#delete-connected-funding-account)


### Generate link token
Same query as it is for connecting a portfolio account. For Trading purposes the purpose must be set to `trading` 
```graphql
query CreatePlaidLinkToken(
    $profile_id: Int!
    $redirect_uri: String!
    $env: String
    $access_token_id: Int
    $purpose: String
) {
  create_plaid_link_token(
    profile_id: $profile_id
    redirect_uri: $redirect_uri
    env: $env
    access_token_id: $access_token_id
    purpose: $purpose
  ){
    link_token
  }
}
```

### Exchange public_token and list accounts
Same query as it is for connecting a portfolio account. For Trading purposes the purpose must be set to `trading`. If purpose is `trading`, then response will contain a list of accounts for the next step. 
```graphql
query LinkPlaidAccount(
    $profile_id: Int!
    $public_token: String!
    $env: String
    $access_token_id: Int
    $purpose: String
) {
  link_plaid_account(
    profile_id: $profile_id
    public_token: $public_token
    env: $env
    access_token_id: $access_token_id
    purpose: $purpose
  ){
    result
    plaid_access_token_id
    accounts {
      account_id
      balance_available
      balance_current
      iso_currency_code
      mask
      name
      official_name
    }
  }
}
```

### Link new funding account
```graphql
mutation TradingLinkBankAccountWithPlaid(
   $profile_id: Int!
   $account_id: String!
   $account_name: String!
   $access_token_id: Int!
) {
   trading_link_bank_account_with_plaid(
      profile_id: $profile_id
      account_id: $account_id
      account_name: $account_name
      access_token_id: $access_token_id
   ){
      funding_account {
         id
         balance
         name
      }
   }
}
```

### List connected funding accounts

With updated balances
```graphql
query TradingGetFundingAccountsWithUpdatedBalance($profile_id: Int!) {
    trading_get_funding_accounts(profile_id: $profile_id) {
        funding_account {
            id
            balance
            name
        }
    }
}
```
With updated balances
```graphql
query TradingGetFundingAccounts($profile_id: Int!) {
    app_trading_funding_accounts(where: {profile_id: {_eq: $profile_id}}) {
        id
        balance
        name
    }
}
```

### Delete connected funding account

```graphql
mutation TradingDeleteFundingAccount(
   $profile_id: Int!
   $funding_account_id: Int!
) {
   trading_delete_funding_account(
      profile_id: $profile_id
      funding_account_id: $funding_account_id
   ) {
      ok
   }
}
```
