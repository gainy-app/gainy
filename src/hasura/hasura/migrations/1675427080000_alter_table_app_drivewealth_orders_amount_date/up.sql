alter table app.drivewealth_orders
    add column total_order_amount_normalized numeric,
    add column date                          date;

update app.drivewealth_orders
set total_order_amount_normalized = case when drivewealth_orders.data ->> 'side' = 'SELL' then -1 else 1 end *
                                    abs((drivewealth_orders.data ->> 'totalOrderAmount')::numeric),
    date                          = (last_executed_at AT TIME ZONE 'America/New_York')::date;
