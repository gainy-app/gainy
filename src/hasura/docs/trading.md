## **[TODO]** KYC
- mutation to fill KYC fields
- action to get KYC form placeholders
- action to send KYC data to DW, store and return KYC status 
- query to get KYC status
- handle files?

## Connect bank account with Plaid
1. [Generate link token](portfolio.md#create-link-token)
2. [Link account](portfolio.md#link-account)
3. Obtain accounts linked to `plaid_access_token_id`
    ```graphql
    query get_token_accounts($token_id: Int!) {
      app_profile_portfolio_accounts(where: {plaid_access_token_id: {_eq: $token_id}}) {
        id
        name
        balance_available
      }
    }
    ```
4. **[TODO]** Link chosen accounts to trading 
    
    Obtain and store processor token.
    ```graphql
    query link_plaid_account_to_trading($profile_id: Int!, $account_id: Int!) {
        link_plaid_account_to_trading(profile_id: $profile_id, account_id: $account_id) {
            result
        }
    }
    ```
   
5. **[TODO]** List connected accounts
   - Including updated balances

## **[TODO]** Deposits / withdrawals
1. Deposit funds
2. Withdraw funds

## **[TODO]** Commissions flow
https://stripe.com/docs/payments/save-and-reuse
1. Create Setup Intent
2. Set up webhook to collect Stripe Payment Methods
3. List payment methods 
4. Set active payment method
5. Background job to charge commissions
6. Handle charging errors
   - send an email?
   - add notification?
   - stop trading?
7. view commissions paid history

## **[TODO]** Trading
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
2. Get actual TTF holding weights and amount
3. Get recommended TTF weights

## **[TODO]** History
1. Get recommended TTF weights
2. Get deposits / withdrawals history with actual statuses 
   
   Not needed if we do transparent account charges


## **[TODO]** Notifications

- KYC status changed
- Commission charged
- Commission charge error

1. Query to get notifications
2. Mutation to mark notifications as read
3. Send notifications through firebase messages
4. Adapt Push Notifications to show them in notification center
5. Get unread notifications count
