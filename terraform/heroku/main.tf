terraform {
  required_providers {
    heroku = {
      source  = "heroku/heroku"
      version = "~> 4.5"
    }
  }
}

variable "name" {
  description = "Name of the Heroku app provisioned"
}

variable "config" {
  type = map(string)
}

variable "addons" {
  type = set(string)
  default = []
}

variable "stack" {
  type = string
}

variable "env" {}

variable "path" {}

variable "buildpacks" {
  type = list(string)
  default = []
}
resource "heroku_app" "app" {
  stack = var.stack
  name   = "${var.name}-${var.env}"
  region = "us"
  sensitive_config_vars = var.config
  buildpacks = var.buildpacks
}

# Build code & release to the app
resource "heroku_build" "build" {
  app        = heroku_app.app.id

  source {
    path     = var.path
  }
}

# Launch the app's web process by scaling-up
resource "heroku_formation" "formation" {
  app        = heroku_app.app.name
  type       = "web"
  quantity   = 1
  size       = "Free"
  depends_on = [heroku_build.build]
}

resource "heroku_addon" "addons" {
  for_each = var.addons
  app = heroku_app.app.name
  plan = each.value
}

output "app_url" {
  value = "https://${heroku_app.app.name}.herokuapp.com"
}
