insert into app.payment_methods (profile_id, name)
select t.profile_id, t.name
from (values (2, 'card visa 4242')) t (profile_id, name)
left join app.payment_methods using (profile_id)
where payment_methods is null;

insert into app.stripe_payment_methods (ref_id, payment_method_id, name, data)
select t.ref_id, payment_methods.id, payment_methods.name, t.data
from (values  ('pm_1LhEWuD1LH0kYxao7AOiSejL', '{"id": "pm_1LhEWuD1LH0kYxao7AOiSejL", "object": "payment_method", "billing_details": {"address": {"city": null, "country": null, "line1": null, "line2": null, "postal_code": null, "state": null}, "email": null, "name": null, "phone": null}, "card": {"brand": "visa", "checks": {"address_line1_check": null, "address_postal_code_check": null, "cvc_check": null}, "country": "US", "exp_month": 9, "exp_year": 2023, "fingerprint": "4iC4R4MJWP7lXxKu", "funding": "credit", "generated_from": null, "last4": "4242", "networks": {"available": ["visa"], "preferred": null}, "three_d_secure_usage": {"supported": true}, "wallet": null}, "created": 1662996396, "customer": "cus_MQ4gYGmV5H4WcR", "livemode": false, "metadata": {}, "type": "card"}'::json)) t (ref_id, data)
join app.payment_methods on payment_methods.profile_id = 2
on conflict do nothing;
