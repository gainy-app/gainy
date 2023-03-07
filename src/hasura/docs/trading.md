# Trading
## API
- [KYC](trading/kyc.md)
- [Connect bank account](trading/connecting_bank_account.md)
- [Deposits / withdrawals](trading/money_flow.md)
- [Trading collections](trading/trading_collections.md)
- [Trading stocks](trading/trading_stocks.md)
- [Commissions](trading/commissions.md)

### History
```graphql
query GetTradingHistory($profile_id: Int!, $types: [String!]!) {
  trading_history(where: {profile_id: {_eq: $profile_id}, type: {_in: $types}}, order_by: {datetime: desc}) {
    amount
    datetime
    name
    tags # ["deposit", "withdraw", "pending", "error", "fee", "ttf", "ticker", "buy", "sell", "cancelled"]
    type
    trading_collection_version { # for TTF orders
      trading_account{
        account_no
      }
      created_at
      status # ["PENDING", "PENDING_EXECUTION", "EXECUTED_FULLY", "CANCELLED", "FAILED"]
      target_amount_delta
      weights
    }
    trading_money_flow { # for deposits / withdrawals
      trading_account{
        account_no
      }
      created_at
      status # ["PENDING", "APPROVED", "SUCCESS", "FAILED"]
      amount
    }
    trading_order { # for ticker orders
      trading_account{
        account_no
      }
      created_at
      status # ["PENDING", "PENDING_EXECUTION", "EXECUTED_FULLY", "CANCELLED", "FAILED"]
      target_amount_delta
    }
  }
}
```
Available types: `["deposit", "withdraw", "trading_fee", "ttf_transaction", "ticker_transaction"]`

May be queried by `money_flow_id`:
```graphql
query GetTradingHistory($profile_id: Int!, $money_flow_id: Int!) {
  trading_history(where: {profile_id: {_eq: $profile_id}, trading_money_flow_id: {_eq: $money_flow_id}}, order_by: {datetime: desc}) {
    ...
  }
}
```

May be queried by `uniq_id`:
```graphql
query GetTradingHistory($profile_id: Int!, $uniq_id: String!) {
  trading_history(where: {profile_id: {_eq: $profile_id}, uniq_id: {_eq: $uniq_id}}) {
    ...
  }
}
```

### Get profile balances and pending transactions
```graphql
query TradingGetProfileStatus($profile_id: Int!) {
  trading_profile_status(where: {profile_id: {_eq: $profile_id}}) {
    buying_power
    deposited_funds
    funding_account_connected
    account_no
    kyc_done
    kyc_status # NOT_READY, READY, PROCESSING, APPROVED, INFO_REQUIRED, DOC_REQUIRED, MANUAL_REVIEW, DENIED
    kyc_message
    kyc_error_messages
    pending_orders_count
    withdrawable_cash
    pending_cash
  }
  app_trading_money_flow(where: {profile_id: {_eq: $profile_id}, status: {_in: ["PENDING", "APPROVED"]}}) {
    amount
    created_at
  }
}
```

### Statements
#### List:
```graphql
query TradingGetStatements($profile_id: Int!) {
  app_trading_statements(where: {profile_id: {_eq: $profile_id}}, order_by: {date: desc}) {
    display_name
    type
    id
    date
  }
}
```
Available types: `["MONTHLY_STATEMENT", "TAX", "TRADE_CONFIRMATION"]`

#### Download:
```graphql
query TradingDownloadStatement($profile_id: Int!, $statement_id: Int!) {
  trading_download_statement(profile_id: $profile_id, statement_id: $statement_id) {
    url
  }
}
```


### Debugging
Sync provider data
```graphql
mutation TradingSyncProviderData($profile_id: Int!) {
  trading_sync_provider_data(profile_id: $profile_id) {
    ok
  }
}
```
Add money to an account
```graphql
mutation TradingAddMoney(
    $profile_id: Int
    $trading_account_id: Int
    $amount: Float!
) {
  trading_add_money(
    profile_id: $profile_id
    trading_account_id: $trading_account_id
    amount: $amount
  ){
    ok
  }
}
```
Delete all trading data for a user
```graphql
mutation TradingDeleteData($profile_id: Int!) {
  trading_delete_data(profile_id: $profile_id) {
    ok
  }
}
```
Re-handle queue messages
```graphql
mutation ReHandleQueueMessages($ids: [Int]!) {
  rehandle_queue_messages(ids: $ids) {
    success
    unsupported
    error
  }
}
```

### **[TODO]** Notifications

- KYC status changed
- Commission charged
- Commission charge error

1. Query to get notifications
2. Mutation to mark notifications as read
3. Send notifications through firebase messages
4. Adapt Push Notifications to show them in notification center
5. Get unread notifications count

## Data
