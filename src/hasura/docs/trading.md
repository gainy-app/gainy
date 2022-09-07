# Trading
## API
- [KYC](trading/kyc.md)
- [Connect bank account](trading/connecting_bank_account.md)
- [Deposits / withdrawals](trading/money_flow.md)
- [Trading collections](trading/trading_collections.md)

### **[TODO]** Commissions flow
https://stripe.com/docs/payments/save-and-reuse
1. Create Setup Intent
   - StripeGetCheckoutUrl with param to change mode to `setup`
2. Set up webhook to collect Stripe Payment Methods
   - StripeWebhook, support new event `checkout.session.completed` and make it not refund everything it sees
   - new action to add payment method with `stripe_session_id` param
3. List payment methods 
   - payment_methods 
4. Set active payment method
   - update payment_methods.set_active_at to now()
5. Background job to create invoices
   - TradingService.update_account_balance(trading_account)
   - TradingService.calculate_commission(trading_account)
   - BillingService.create_invoice(profile_id, amount, description)
     - due date in future
     - specify period and check there is no invoice for this period
     - amount = commission * period days / 365
6. Background job to pay invoices
   - Get unpaid invoices, invoices with charge errors and changed payment methods 
   - BillingService.create_payment(invoice)
     - thread safe 
     - PaymentService.charge(invoice)
     - save result
7. Handle charging errors
   - send an email?
   - add notification?
   - stop trading?
8. view commissions paid history

Data used: payment_methods, invoices, invoice_payments

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

- payment_methods 
  - id: int
  - profile_id: int
  - name: string
  - stripe_ref_id: string
  - set_active_at: datetime

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
