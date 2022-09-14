# Trading
## API
- [KYC](trading/kyc.md)
- [Connect bank account](trading/connecting_bank_account.md)
- [Deposits / withdrawals](trading/money_flow.md)
- [Trading collections](trading/trading_collections.md)
- [Commissions](trading/commissions.md)

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
    $profile_id: Int!
    $trading_account_id: Int!
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

### **[TODO]** History
1. Get rebalancing history (trading_collection_versions with status `complete`) 
2. Get deposits / withdrawals history with actual statuses (trading_money_flow) 
3. Get commission payment history 


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

- invoices
  - id: int
  - profile_id: int
  - amount: int
  - due_date: timestamp
  - description: string
  - period_start: date
  - period_end: date
  - metadata: json

- invoice_payments
  - id: int
  - profile_id: int
  - invoice_id: int
  - result: boolean
  - response: json

## SQS

1. Update `trading_collection_versions` status on `drivewealth_autopilot_run` execution
2. Update `trading_money_flow` status on deposit / withdrawal execution

## Questions

KYC:
- What's COMPLIANCE_AML_INFO
