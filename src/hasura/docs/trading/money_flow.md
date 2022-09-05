# Queries

1. [Deposit funds](#deposit-funds)
2. [Withdraw funds](#withdraw-funds)
3. Rebalance Portfolio in both cases
   - TradingService.on_deposit(profile_id, amount_cents)
      - Update Portfolio status
         - Portfolio will map to `drivewealth_portfolios`
      - Rebalance Portfolio funds
         - cash: `target = actual`

Data used:
- drivewealth_portfolios
- trading_money_flow

### Deposit funds
Same query as it is for connecting a portfolio account. For Trading purposes the purpose must be set to `trading` 
```graphql
mutation TradingDepositFunds(
    $profile_id: Int!
    $trading_account_id: Int!
    $amount_cents: Int!
    $funding_account_id: Int!
) {
    trading_deposit_funds(
        profile_id: $profile_id
        trading_account_id: $trading_account_id
        amount_cents: $amount_cents
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
    $amount_cents: Int!
    $funding_account_id: Int!
) {
    trading_withdraw_funds(
        profile_id: $profile_id
        trading_account_id: $trading_account_id
        amount_cents: $amount_cents
        funding_account_id: $funding_account_id
    ) {
        trading_money_flow_id
    }
}
```
