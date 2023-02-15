data "aws_elb_service_account" "main" {}
data "aws_acm_certificate" "sslcert" {
  domain = "*.${var.domain}"
}

resource "aws_s3_bucket" "lb_logs" {
  bucket = "loadbalancer-${var.name}-${var.env}"
  tags = {
    Name = "Load balancer logs"
  }
}
resource "aws_s3_bucket_ownership_controls" "lb_logs" {
  bucket = aws_s3_bucket.lb_logs.bucket
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}
resource "aws_s3_bucket_server_side_encryption_configuration" "lb_logs" {
  bucket = aws_s3_bucket.lb_logs.bucket
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_policy" "lb-bucket-policy" {
  bucket = aws_s3_bucket.lb_logs.id
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "AWS" : data.aws_elb_service_account.main.arn
        },
        "Action" : "s3:PutObject",
        "Resource" : "${aws_s3_bucket.lb_logs.arn}/*"
      }
    ]
  })
}

/*
 * Create application load balancer
 */
resource "aws_alb" "alb" {
  name            = "alb-${var.name}-${var.env}"
  internal        = false
  security_groups = [var.vpc_default_sg_id, var.public_https_sg_id, var.public_http_sg_id]
  subnets         = var.public_subnet_ids

  access_logs {
    bucket  = aws_s3_bucket.lb_logs.bucket
    prefix  = "${var.name}-${var.env}"
    enabled = true
  }
}
/*
 * Create target group for ALB
 */
resource "aws_alb_target_group" "default" {
  name        = "tg-${var.name}-${var.env}"
  port        = "80"
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  stickiness {
    type = "lb_cookie"
  }
  health_check {
    interval = 60
    matcher  = "200,302"
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
    type = "redirect"

    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
  tags = {
    Name = "${var.name}-${var.env}"
  }
}

/*
 * Create Cloudflare DNS record
 */
resource "cloudflare_record" "service" {
  name    = "${var.name}-${var.env}"
  value   = aws_alb.alb.dns_name
  type    = "CNAME"
  proxied = false
  zone_id = var.cloudflare_zone_id
}

output "url" {
  value = cloudflare_record.service.hostname
}
output "aws_alb" {
  value = aws_alb.alb
}
output "aws_alb_target_group" {
  value = aws_alb_target_group.default
}