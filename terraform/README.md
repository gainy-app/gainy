# Terraforming of Gainy infra

Terraform is run from the Github workflows. 

To deploy the service follow these steps:
1. Setup up environment variables. Refer to the [workflow config](.github/workflows/terraform.yml) for the full list of needed variables. 
2. `terraform login # log in to Terraform Cloud`
3. `terraform plan # run to see the changes in terraform` 
4. `terraform apply # apply the changes infra changes` 

Key features:
- all services are isolated in a VPC (except lambdas - they are still in public, we’ll need to address this in future). An EC2 instance is created as proxy to enable interacting with the DB via ssh tunnel
- meltano and hasura are running in ECS cluster. Each deployment a new service is started, then the old one is destroyed.

### New environments
1. Separate environments are configured via different backend configs:
```
terraform init -backend-config=backend-production.hcl -reconfigure
```
2. Now head to terraform cloud and change execution mode from remote to local.

# Troubleshooting

Issue: terraform state is locked

Description: upon execution terraform locks global state so that only one infra change can be executed at a time. I case when execution fails sometimes lock remains and has to be removed manually. 

Solution: go to https://app.terraform.io/app/gainy/workspaces/gainy-dev/settings/lock to manually unlock the state.

**WARNING**: in normal circumstances you should not be doing this. It only occurs when the CI/CD is failing or we made an error in the terraform scripts. Before unlocking make sure that noone actually executing a legit terraform plan by asking in #dev slack channel.
