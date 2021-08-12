variable "google_project_id" {}
variable "google_billing_id" {}
variable "google_user" {}
variable "google_organization_id" {}

resource "google_project" "project" {
  name            = "Gainy App CI"
  project_id      = var.google_project_id
  billing_account = var.google_billing_id
  org_id          = var.google_organization_id
}
resource "google_project_iam_member" "owner" {
  role   = "roles/owner"
  member = "user:${var.google_user}"

  depends_on = [google_project.project]
}

# Enable Compute API
resource "google_project_service" "compute" {
  service    = "compute.googleapis.com"
  depends_on = [google_project.project]
}

# Enable Cloud Functions API
resource "google_project_service" "cf" {
  project = var.google_project_id
  service = "cloudfunctions.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = false
  depends_on                 = [google_project.project]
}

# Enable Cloud Build API
resource "google_project_service" "cb" {
  project = var.google_project_id
  service = "cloudbuild.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = false
  depends_on                 = [google_project.project]
}

module "functions-processSignUp" {
  source               = "./functions"
  function_entry_point = "processSignUp"
  function_name        = "process_signup"
  google_project_id    = var.google_project_id
  depends_on           = [google_project_service.cf, google_project_service.cb, google_project_service.compute]
}

module "functions-refreshToken" {
  source               = "./functions"
  function_entry_point = "refreshToken"
  function_name        = "refresh_token"
  google_project_id    = var.google_project_id
  depends_on           = [google_project_service.cf, google_project_service.cb, google_project_service.compute]
}