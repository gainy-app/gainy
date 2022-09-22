# Commissions
https://stripe.com/docs/payments/save-and-reuse
https://stripe.com/docs/payments/payment-intents/upgrade-to-handle-actions

1. [Prepare payment sheet](#prepare-payment-sheet)
2. [List payment methods](#list-payment-methods) 
3. [Set active payment method](#set-active-payment-method)
4. [Delete payment method](#delete-payment-method)
5. Background job to create invoices - [gainy_create_invoices](https://github.com/gainy-app/gainy-compute/gainy/billing/jobs/create_invoices.py)
6. Background job to pay invoices - [gainy_charge_invoices](https://github.com/gainy-app/gainy-compute/gainy/billing/jobs/charge_invoices.py)
7. [TODO] Handle charging errors
   - charge again 1-2-3 times in a day
   - charge again when payment method is updated
   - send an email?
   - add notification?
   - stop trading?
8. [TODO] view commissions paid history


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
