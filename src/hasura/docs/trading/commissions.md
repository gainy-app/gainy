# Commissions
https://stripe.com/docs/payments/save-and-reuse

1. [Prepare payment sheet](#prepare-payment-sheet)
2. [List payment methods](#list-payment-methods) 
3. [Set active payment method](#set-active-payment-method)
4. [Delete payment method](#delete-payment-method)
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


### Prepare payment sheet
```graphql
query StripeGetPaymentSheetData($profile_id: Int!) {
  stripe_get_payment_sheet_data(profile_id: $profile_id) {
    customer_id
    setup_intent_client_secret
    ephemeral_key
    publishable_key
  }
}
```

### List payment methods
```graphql
query GetPaymentMethods($profile_id: Int!) {
  app_payment_methods(
    where: {profile_id: {_eq: $profile_id}}, 
    order_by: [
        {set_active_at: desc_nulls_last}, 
        {created_at: desc}
    ]
  ) {
    id
    name
  }
}
```

### Set active payment method
```graphql
mutation SetPaymentMethodActive($payment_method_id: Int!) {
  update_app_payment_methods_by_pk(
    pk_columns: {id: $payment_method_id}, 
    _set: {set_active_at: now}
  ){
    id
  }
}
```


### Delete payment method
```graphql
mutation DeletePaymentMethod($payment_method_id: Int!) {
  delete_app_payment_methods_by_pk(id: $payment_method_id){
    id
  }
}
```
