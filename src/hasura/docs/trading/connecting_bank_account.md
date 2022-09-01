# Queries

1. [Generate link token](#generate-link-token)
2. [Exchange public_token and list accounts](#exchange-public_token-and-list-accounts)
3. [Link Account to DriveWealth](#link-account-to-drivewealth)
4. **[TODO]** List connected accounts
  - PlaidService.updateAccountBalance(plaid_account_ids)
  - list managed_portfolio_bank_accounts

5. **[TODO]** Deny deleting plaid tokens connected to trading

6. **[TODO]** Disconnect bank account
  - TradingService.disconnectBankAccount(profile_id, trading_bank_account)
  - remove trading_bank_account


### Generate link token
Same query as it is for connecting a portfolio account. For Trading purposes the purpose must be set to `managed_trading` 
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
Same query as it is for connecting a portfolio account. For Trading purposes the purpose must be set to `managed_trading`. If purpose is `managed_trading`, then response will contain a list of accounts for the next step. 
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
      balance_currency
      mask
      name
      official_name
    }
  }
}
```


### Link Account to DriveWealth
```graphql
mutation LinkManagedTradingBankAccountWithPlaid(
   $profile_id: Int!
   $account_id: String!
   $account_name: String!
   $access_token_id: Int!
) {
   link_managed_trading_bank_account_with_plaid(
      profile_id: $profile_id
      account_id: $account_id
      account_name: $account_name
      access_token_id: $access_token_id
   ){
      error_message
   }
}

```
