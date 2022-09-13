# Trading / Deposits & withdrawals

### Deposit funds
Same query as it is for connecting a portfolio account. For Trading purposes the purpose must be set to `trading` 
```graphql
mutation TradingDepositFunds(
    $profile_id: Int!
    $trading_account_id: Int!
    $amount: Float!
    $funding_account_id: Int!
) {
    trading_deposit_funds(
        profile_id: $profile_id
        trading_account_id: $trading_account_id
        amount: $amount
        funding_account_id: $funding_account_id
    ) {
        trading_money_flow_id
    }
}
```

### Withdraw funds
Same query as it is for connecting a portfolio account. For Trading purposes the purpose must be set to `trading` 
```graphql
mutation TradingWithdrawFunds(
    $profile_id: Int!
    $trading_account_id: Int!
    $amount: Float!
    $funding_account_id: Int!
) {
    trading_withdraw_funds(
        profile_id: $profile_id
        trading_account_id: $trading_account_id
        amount: $amount
        funding_account_id: $funding_account_id
    ) {
        trading_money_flow_id
    }
}
```
