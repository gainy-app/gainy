resource "stripe_webhook_endpoint" "webhook" {
  url = var.webhook_url
  enabled_events = [
    "payment_method.updated",
    "payment_method.detached",
    "payment_method.automatically_updated",
    "payment_method.attached",
    "payment_intent.succeeded",
  ]
}
