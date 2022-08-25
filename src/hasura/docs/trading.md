# Trading
## API
### **[TODO]** [KYC](trading/kyc.md)

### Connect bank account with Plaid
1. [Generate link token](portfolio.md#create-link-token) with additional param to know it's for trading and not for portfolio
2. [Link account](portfolio.md#link-account) with additional param to know it's for trading and not for portfolio, make it synchronous, sync investments / return accounts in this case, remove plaid trigger. 
3. **[TODO]** Link chosen accounts to trading 
    
    Obtain and store processor token.
    ```graphql
    query link_plaid_account_to_trading($profile_id: Int!, $account_id: Int!) {
        link_plaid_account_to_trading(profile_id: $profile_id, account_id: $account_id) {
            result
        }
    }
    ```
    - TradingService.connectBankAccount(profile_id, processorToken)
    - Create trading_bank_accounts

4. **[TODO]** List connected accounts
   - PlaidService.updateAccountBalance(plaid_account_ids)
   - list trading_bank_accounts

5. **[TODO]** Deny deleting plaid tokens connected to trading

6. **[TODO]** Disconnect bank account
   - TradingService.disconnectBankAccount(profile_id, trading_bank_account)
   - remove trading_bank_account

Data used: trading_bank_accounts, drivewealth_bank_accounts

### **[TODO]** Deposits / withdrawals
1. Deposit funds
   - TradingService.depositFunds(profile_id, trading_account, amount, trading_bank_account)
2. Withdraw funds
   - TradingService.withdrawFunds(profile_id, trading_account, amount, trading_bank_account)

Data used: trading_accounts, trading_bank_accounts, drivewealth_bank_accounts

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

Data used: payment_methods, invoice, invoice_payment

### **[TODO]** Trading
1. Reconfigure TTF holdings

    Generate the trades to make user's TTF holdings look like input params.
    ```graphql
    input TickerWeight {
        symbol: String!
        weight: Float!    
    }
    mutation reconfigure_ttf_holdings($profile_id: Int!, $account_id: Int!, $collection_id: Int!, $weights: [TickerWeight], $amount_cents: Int) {
        reconfigure_ttf_holdings(profile_id: $profile_id, account_id: $account_id, collection_id: $collection_id, weights: $weights, amount_cents: $amount_cents) {
            result
        }
    }
    ```
   - CollectionTradingService.get_latest_collection_version(profile_id)
   - CollectionTradingService.create_new_collection_version(profile_id, $collection_id, $weights)
   - CollectionTradingService.calculate_operation_set(old_version, new_version)
     - if no old version - just buy new version amounts
     - if there is an old version but with no executed trades:
       - cancel orders
       - 
2. Get actual TTF holding weights and amount
3. Get recommended TTF weights

### **[TODO]** History
1. Get recommended TTF weights
2. Get deposits / withdrawals history with actual statuses 
   
   Not needed if we do transparent account charges


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

- trading_bank_accounts 
  - id: int
  - profile_id: int
  - plaid_account_id: int
  - name: string
  - balance: int

- drivewealth_bank_accounts 
  - id: int
  - ref_id: string
  - drivewealth_user_id: int
  - trading_account_id: int
  - bankAccountNickname: string
  - bankAccountNumber: string
  - bankRoutingNumber: string
  - bankAccountType: string

- invoice
  - id: int
  - profile_id: int
  - amount_cents: int
  - due_date: timestamp
  - description: string
  - period_start: date
  - period_end: date
  - metadata: json

- invoice_payment
  - id: int
  - profile_id: int
  - invoice_id: int
  - result: boolean
  - response: json

## Questions

KYC:
- What's COMPLIANCE_AML_INFO
- Can we hide extended hours agreement?
- We need Links to all disclosures
- We need Links to all disclosures
Plaid:
- Which plaid products to use? Possible values: `assets, auth, employment, identity, income_verification, identity_verification, investments, liabilities, payment_initiation, standing_orders, transactions, transfer` 
- How to send plaid processor_token to Create Bank Account API?
Deposits:
- Right approach of funding multiple accounts when using autopilot
Trading:
- How to rebalance current position (current position has a number of stocks and we would like to make it up to an amount of money)
- 