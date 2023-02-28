alter table app.drivewealth_accounts
    add column "payment_method_id" int,
    add foreign key ("payment_method_id") REFERENCES "app"."payment_methods" ("id") ON UPDATE cascade ON DELETE set null;

with accounts as
         (
             select distinct on (profile_id) profile_id, ref_no, drivewealth_accounts.ref_id
             from app.drivewealth_accounts
                      join app.drivewealth_users
                           on drivewealth_accounts.drivewealth_user_id = drivewealth_users.ref_id
             where drivewealth_accounts.status = 'OPEN'
               and payment_method_id is null
         ),
     new_payment_methods as
         (
             insert into app.payment_methods (profile_id, name, provider)
                 select profile_id, 'Trading Account ' || ref_no, 'DRIVEWEALTH'
                 from accounts
                 returning id, profile_id
     )
update app.drivewealth_accounts
set payment_method_id = new_payment_methods.id
from new_payment_methods,
     accounts
where accounts.ref_id = drivewealth_accounts.ref_id
  and accounts.profile_id = new_payment_methods.profile_id