variable "env" {}
variable "google_project_id" {}
variable "google_billing_id" {}
variable "google_user" {}
variable "google_organization_id" {}

locals {
  google_project_id = var.env == "production" ? var.google_project_id : "${var.google_project_id}-${var.env}"
}

resource "google_project" "project" {
  name            = var.env == "production" ? "Gainy App CI" : "Gainy App CI ${var.env}"
  project_id      = local.google_project_id
  billing_account = var.google_billing_id
  org_id          = var.google_organization_id
  lifecycle {
    prevent_destroy = true
  }
}
resource "google_project_iam_member" "owner" {
  project = local.google_project_id
  role    = "roles/owner"
  member  = "user:${var.google_user}"

  depends_on = [google_project.project]
  lifecycle {
    prevent_destroy = true
  }
}

# Enable Compute API
resource "google_project_service" "compute" {
  service    = "compute.googleapis.com"
  depends_on = [google_project.project]
  lifecycle {
    prevent_destroy = true
  }
}

# Enable Cloud Functions API
resource "google_project_service" "cf" {
  project = local.google_project_id
  service = "cloudfunctions.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = false
  depends_on                 = [google_project.project]
  lifecycle {
    prevent_destroy = true
  }
}

# Enable Cloud Build API
resource "google_project_service" "cb" {
  project = local.google_project_id
  service = "cloudbuild.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = false
  depends_on                 = [google_project.project]
  lifecycle {
    prevent_destroy = true
  }
}

module "functions-processSignUp" {
  source               = "./functions"
  function_entry_point = "processSignUp"
  function_name        = "process_signup"
  google_project_id    = local.google_project_id
  depends_on           = [google_project_service.cf, google_project_service.cb, google_project_service.compute]
}

module "functions-refreshToken" {
  source               = "./functions"
  function_entry_point = "refreshToken"
  function_name        = "refresh_token"
  google_project_id    = local.google_project_id
  depends_on           = [google_project_service.cf, google_project_service.cb, google_project_service.compute]
}


# Enable Cloud Places API
#resource "google_project_service" "places-backend" {
#  project = google_project.project.project_id
#  service = "places-backend.googleapis.com"
#
#  disable_dependent_services = true
#  disable_on_destroy         = true
#}
resource "google_project_service" "geocoding-backend" {
  project = google_project.project.project_id
  service = "geocoding-backend.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = true
}

output "google_project_id" {
  value = local.google_project_id
}
