# Portfolio dbt models

1. portfolio_chart - (deprecated) holdings X historical_prices_aggregated
2. portfolio_expanded_transactions - a number of heuristics to fill missing transactions
3. portfolio_gains - overall portfolio gains for profiles
4. portfolio_holding_details - calculates data for filtering and sorting of holdings in the app
5. portfolio_holding_gains - calculates gains over time for each holding
6. portfolio_holding_group details and gains - just a sum of the two entities above grouped by ticker
7. portfolio_securities_normalized - normalized securities with link to tickers
8. portfolio_transaction_chart - chart records for each transaction (used in mobile app to calculate portfolio chart)
9. portfolio_transaction_gains - calculates gains over time for each transaction
10. profile_holding_groups - holding groups to show in the app
11. profile_holdings_normalized - holdings to show in the app
