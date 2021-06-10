terraform {
  required_providers {
    heroku = {
      source  = "heroku/heroku"
      version = "~> 4.0"
    }
  }
}

variable "name" {
  description = "Name of the Heroku app provisioned"
}

variable "config" {
  type = map(string)
}

variable "env" {}

variable "path" {}

resource "heroku_app" "app" {
  stack = "container"
  name   = "${var.name}-${var.env}"
  region = "us"
  sensitive_config_vars = var.config
}

# Build code & release to the app
resource "heroku_build" "build" {
  app        = heroku_app.app.name

  source {
    path     = var.path
  }
}

# Launch the app's web process by scaling-up
resource "heroku_formation" "formation" {
  app        = heroku_app.app.name
  type       = "web"
  quantity   = 1
  size       = "Hobby"
  depends_on = [heroku_build.build]
}

output "app_url" {
  value = "https://${heroku_app.app.name}.herokuapp.com"
}