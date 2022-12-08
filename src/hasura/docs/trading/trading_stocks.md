# Trading collections
1. [Create Stock order](#create-stock-order)
2. [Cancel pending TTF order](#cancel-pending-ttf-order)
3. [Get actual TTF holding weights and amount](#get-actual-ttf-holding-weights-and-amount)
4. [Get actual TTF amount and history](#get-actual-ttf-amount-and-history)


### Trading mechanism
- All the requests are in PENDING state until 9.30am EST
- At 9.30 am they're sent to DW, status changes to PENDING_EXECUTION
- At 10.00 they're executed by DW
- From 10 to 11 we're getting events from DW _and updating TCS statuses_
- At 17 we download all accounts _and portfolios information_

### Create Stock order
Generate the trades to make user's TTF holdings look like input params.
```graphql
mutation TradingCreateStockOrder(
    $profile_id: Int!
    $symbol: String!
    $target_amount_delta: Float!
) {
  trading_create_stock_order(
    profile_id: $profile_id
    symbol: $symbol
    target_amount_delta: $target_amount_delta
  ){
    trading_order_id
  }
}
```

### Cancel pending stock order
```graphql
mutation TradingCancelPendingOrder(
    $profile_id: Int!
    $trading_order_id: Int!
) {
  trading_cancel_pending_order(
    profile_id: $profile_id
    trading_order_id: $trading_order_id
  ){
    trading_order_id
  }
}
```

### Get actual stock amount and history
```graphql
query TradingGetProfileStockStatus($profile_id: Int!, $symbol: String!) {
  trading_profile_stock_status(where: {profile_id: {_eq: $profile_id}, symbol: {_eq: $symbol}}) {
    absolute_gain_1d
    absolute_gain_total
    actual_value
    relative_gain_1d
    relative_gain_total
    value_to_portfolio_value
  }
  app_trading_orders(where: {profile_id: {_eq: $symbol}, symbol: {_eq: $symbol}}, limit: 3, order_by: {created_at: desc}) {
    id
    created_at
    target_amount_delta
    history{
      tags
    }
  }
}
```
