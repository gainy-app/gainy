# Trading
## API
- [KYC](trading/kyc.md)
- [Connect bank account](trading/connecting_bank_account.md)
- [Deposits / withdrawals](trading/money_flow.md)
- [Trading collections](trading/trading_collections.md)
- [Commissions](trading/commissions.md)

### History
```graphql
query GetTradingHistory($profile_id: Int!, $types: [String!]!) {
  trading_history(where: {profile_id: {_eq: $profile_id}, type: {_in: $types}}, order_by: {datetime: desc}) {
    amount
    datetime
    name
    tags
    type
  }
}
```
Available types: `["deposit", "withdraw", "trading_fee", "ttf_transaction"]`

### Get actual balances and pending transactions
```graphql
query TradingGetProfileData($profile_id: Int!) {
  trading_get_profile_data(profile_id: $profile_id) {
    history {
      pending {
        created_at
        amount
      }
    }
    withdrawable_cash
    buying_power
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
