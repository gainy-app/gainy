data "aws_acm_certificate" "sslcert" {
  domain = "*.${var.domain}"
}

/*
 * Create application load balancer
 */
resource "aws_alb" "alb" {
  name            = "alb-${var.name}-${var.env}"
  internal        = false
  security_groups = [var.vpc_default_sg_id, var.public_https_sg_id, var.public_http_sg_id]
  subnets         = var.public_subnet_ids
}
/*
 * Create target group for ALB
 */
resource "aws_alb_target_group" "default" {
  name     = "tg-${var.name}-${var.env}"
  port     = "80"
  protocol = "HTTP"
  vpc_id   = var.vpc_id
  stickiness {
    type = "lb_cookie"
  }
  health_check {
    interval = 60
    matcher = "200,302"
  }
}
/*
 * Create listeners to connect ALB to target group
 */
resource "aws_alb_listener" "https" {
  load_balancer_arn = aws_alb.alb.arn
  port              = "443"
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-2016-08"
  certificate_arn   = data.aws_acm_certificate.sslcert.arn
  default_action {
    target_group_arn = aws_alb_target_group.default.arn
    type             = "forward"
  }
  tags = {
    Name = "${var.name}-${var.env}"
  }
}
resource "aws_alb_listener" "http" {
  load_balancer_arn = aws_alb.alb.arn
  port              = "80"
  protocol          = "HTTP"
  default_action {
    target_group_arn = aws_alb_target_group.default.arn
    type             = "forward"
  }
  tags = {
    Name = "${var.name}-${var.env}"
  }
}

/*
 * Create ECS Service
 */
resource "aws_ecs_service" "service" {
  name                               = "${var.name}-${var.env}"
  cluster                            = var.ecs_cluster_name
  desired_count                      = 1 #length(module.ecs.aws_zones)
  iam_role                           = var.ecs_service_role_arn
  deployment_maximum_percent         = "200"
  deployment_minimum_healthy_percent = "0"

  ordered_placement_strategy {
    type  = "spread"
    field = "instanceId"
  }

  load_balancer {
    target_group_arn = aws_alb_target_group.default.arn
    container_name   = var.name
    container_port   = var.container_port
  }

  task_definition      = var.task_definition
  force_new_deployment = true
}

/*
 * Create Cloudflare DNS record
 */
resource "cloudflare_record" "service" {
  name    = "${var.name}-${var.env}"
  value   = aws_alb.alb.dns_name
  type    = "CNAME"
  proxied = true
  zone_id = var.cloudflare_zone_id
}

output "aws_alb_url" {
  value = aws_alb.alb.dns_name
}