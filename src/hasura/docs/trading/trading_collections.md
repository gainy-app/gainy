### Trading collections
1. Get recommended TTF weights
2. Reconfigure TTF holdings

   Generate the trades to make user's TTF holdings look like input params.
    ```graphql
    input TickerWeight {
        symbol: String!
        weight: Float!    
    }
    mutation reconfigure_ttf_holdings($profile_id: Int!, $collection_id: Int!, $weights: [TickerWeight], $target_amount_delta: Float) {
        reconfigure_ttf_holdings(profile_id: $profile_id, collection_id: $collection_id, weights: $weights, target_amount_delta: $target_amount_delta) {
            result
        }
    }
    ```
    - TradingService.reconfigure_ttf_holdings(profile_id, collection_id, weights, target_amount_delta)
        - Create new trading_collection_versions, trading_collection_contents
        - Update account buying power, Portfolio status
        - Create or update Fund
        - Calculate `relative_weight_change`
            - Positive: `relative_weight_change = target_amount_delta / CASH_RESERVE value`
            - Negative: `relative_weight_change = target_amount_delta / updated FUND value`
        - Rebalance Portfolio funds
            - cash: decrease by relative_weight_change
            - updated Fund: increase by relative_weight_change
        - Create autopilot run
3. Get actual TTF holding weights and amount

Data used:
- drivewealth_autopilot_run