### Get Checkout URL

```graphql
{
    stripe_get_checkout_url(price_id: "price_1LQ8ugD1LH0kYxaoP9gtuEa0", success_url: "http://example.com/success.html", cancel_url: "http://example.com/cancel.html") {
        url
    }
}
```

After the payment is done, it's refunded by the webhook.
