ALTER TABLE "app"."stripe_payment_intents"
    drop constraint stripe_payment_intents_payment_transaction_id_fkey,
    drop column payment_transaction_id;
