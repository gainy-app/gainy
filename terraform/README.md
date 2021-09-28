# Terraforming of Gainy infra

## Note on ./src sim link 

The /terraform/src -> ../src simlink is a workaround for a known issue
https://github.com/heroku/terraform-provider-heroku/issues/269

# Prerequisites
You have to cd into `./terraform` folder before executing terraform commands

# Setup env vars
1. Grab an api key from eodhistoricaldata.com (find creds in 1password) 
2. [Get AWS credentials](https://docs.aws.amazon.com/singlesignon/latest/userguide/howtogetcredentials.html)
3. [Install heroku cli](https://devcenter.heroku.com/articles/heroku-cli)

4. Run in terminal to plan and deploy changes via terraform 
```bash
# initialize configuration variables. Needs to be done only once
export TF_VAR_eodhistoricaldata_api_token=`{token here from #1}`
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=${key here from #2}
export AWS_SECRET_ACCESS_KEY=${secret key here from #2}
terraform login # to login into terraform cloud

# run the following to see the changes in terraform
terraform plan

# apply the changes infra changes
terraform apply
```

### New environments

Key features:
- all services are isolated in a VPC (except lambdas - they are still in public, I’ll address it in future). An EC2 instance is created as proxy to enable interacting with the DB via ssh tunnel
- all services are designed to be deployed and destroyed separately of each other (this is not checked yet) - except for the firebase auth, currently all future environments are meant to share all google services
- meltano and hasura are running in ECS cluster. Each deployment a new service is started, then the old one is destroyed (reducing downtime)
- terraform is set to not use foreign key relationships - all relationship were converted to manual type (otherwise hasura fails during dbt run). However other hasura failures are still subject to investigation

Production deployment:
```
terraform init -backend-config=backend-production.hcl -reconfigure
# Now head to terraform and change execution mode from remote to local. TODO: place for improvement
terraform apply 
# At this point you will deploy something, but fail with google resources, so you need to import them.
# Unfortunately currently it's impossible to import first due to docker provider's configuration requires
# values that will be known after apply. TODO: place for improvement (or separate test firebase from production firebase)
./import-shared-resources.sh
terraform apply
```

# Troubleshooting

Issue: terraform state is locked

Description: upon execution terraform locks global state so that only one infra change can be executed at a time. I case when execution fails sometimes lock remains and has to be removed manually. 

Solution: go to https://app.terraform.io/app/gainy/workspaces/gainy-dev/settings/lock to manually unlock the state.

**WARNING**: in normal circumstances you should not be doing this. It only occurs when the CI/CD is failing or we made an error in the terraform scripts. Before unlocking make sure that noone actually executing a legit terraform plan by asking in #dev slack channel.
