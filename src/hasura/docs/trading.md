# Trading
## API
- [KYC](trading/kyc.md)
- [Connect bank account](trading/connecting_bank_account.md)

### **[TODO]** Deposits / withdrawals
1. Deposit funds
   - TradingService.depositFunds(profile_id, trading_account, amount, trading_bank_account)
2. Withdraw funds
   - TradingService.withdrawFunds(profile_id, trading_account, amount, trading_bank_account)
3. Rebalance Portfolio in both cases
   - TradingService.on_deposit(profile_id, amount_cents)
     - Update Portfolio status
        - Portfolio will map to `drivewealth_portfolios`
     - Rebalance Portfolio funds
       - cash: `target = actual`

Data used: 
- managed_portfolio_trading_accounts 
- managed_portfolio_bank_accounts
- drivewealth_bank_accounts 
- drivewealth_portfolios
- managed_portfolio_money_flow

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

### **[TODO]** Trading
1. Get recommended TTF weights
2. Reconfigure TTF holdings

    Generate the trades to make user's TTF holdings look like input params.
    ```graphql
    input TickerWeight {
        symbol: String!
        weight: Float!    
    }
    mutation reconfigure_ttf_holdings($profile_id: Int!, $account_id: Int!, $collection_id: Int!, $weights: [TickerWeight], $absolute_amount_delta_cents: Int, $relative_amount_delta_percent: Int) {
        reconfigure_ttf_holdings(profile_id: $profile_id, account_id: $account_id, collection_id: $collection_id, weights: $weights, absolute_amount_delta_cents: $absolute_amount_delta_cents, relative_amount_delta_percent: $relative_amount_delta_percent) {
            result
        }
    }
    ```
   - TradingService.reconfigure_ttf_holdings(profile_id, collection_id, weights, absolute_amount_delta_cents, relative_amount_delta_percent)
     - Create new managed_portfolio_collection_versions, managed_portfolio_collection_contents 
     - Update account buying power, Portfolio status
     - Create or update Fund
     - Calculate `relative_weight_change`
       - from `absolute_amount_delta_cents` 
         - Positive: `relative_weight_change = absolute_amount_delta_cents / CASH_RESERVE value` 
         - Negative: `relative_weight_change = absolute_amount_delta_cents / updated FUND value` 
       - from `relative_amount_delta_percent`
         - Positive: `relative_weight_change = relative_amount_delta_percent / 100 * CASH_RESERVE actual weight` 
         - Negative: `relative_weight_change = relative_amount_delta_percent / 100 * updated FUND actual weight` 
     - Rebalance Portfolio funds
       - cash: decrease by relative_weight_change
       - updated Fund: increase by relative_weight_change
     - Create autopilot run
3. Get actual TTF holding weights and amount

Data used: 
- managed_portfolio_collection_versions
- managed_portfolio_collection_contents
- drivewealth_portfolios
- drivewealth_accounts
- drivewealth_funds
- drivewealth_autopilot_run

### **[TODO]** History
1. Get rebalancing history (managed_portfolio_collection_versions with status `complete`) 
2. Get deposits / withdrawals history with actual statuses (managed_portfolio_money_flow) 
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
  - amount_cents: int
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

- managed_portfolio_collection_versions:
  - id: int
  - profile_id: int
  - collection_uniq_id: string
  - targetAmount: int
  - actualAmount: int
  - status: string

- managed_portfolio_collection_contents:
  - id: int
  - managed_portfolio_collection_version_id: int
  - symbol: string
  - target_weight: numeric

- drivewealth_portfolios
  - ref_id: string
  - drivewealth_account_id: int
  - raw_data: json
  - cash_target: numeric
  - cash_actual: numeric
  - cash_value: numeric

- drivewealth_funds
  - ref_id: string
  - managed_portfolio_collection_version_id: int
  - raw_data: json

- drivewealth_autopilot_run
  - ref_id: string
  - managed_portfolio_collection_version_id: int
  - status: string
  - drivewealth_account_id: int
  - raw_data: json

- managed_portfolio_money_flow
  - id: int
  - profile_id: int
  - amount: int
  - trading_account_id: int
  - status: string

## SQS

1. Update `managed_portfolio_collection_versions` status on `drivewealth_autopilot_run` execution
2. Update `managed_portfolio_money_flow` status on deposit / withdrawal execution

## Questions

KYC:
- What's COMPLIANCE_AML_INFO
- Can we hide extended hours agreement?
- We need Links to all disclosures
Plaid:
- Which plaid products to use? Possible values: `assets, auth, employment, identity, income_verification, identity_verification, investments, liabilities, payment_initiation, standing_orders, transactions, transfer` 
- How to send plaid processor_token to Create Bank Account API?
Deposits:
- Right approach of funding multiple accounts when using autopilot
Trading:
- How to rebalance current position (current position has a number of stocks and we would like to make it up to an amount of money)
- 