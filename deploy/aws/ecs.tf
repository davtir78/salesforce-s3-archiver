resource "aws_ecr_repository" "this" {
  name                 = var.name
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/ecs/${var.name}"
  retention_in_days = 7
}

resource "aws_ecs_cluster" "this" {
  name = var.name
  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

resource "aws_service_discovery_private_dns_namespace" "this" {
  name = "${var.name}.internal"
  vpc  = aws_vpc.this.id
}

resource "aws_service_discovery_service" "mock" {
  count = var.use_mock_salesforce ? 1 : 0
  name  = "mock-salesforce"
  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.this.id
    routing_policy = "MULTIVALUE"
    dns_records {
      ttl  = 10
      type = "A"
    }
  }
}

resource "aws_ssm_parameter" "sf_client_secret" {
  name  = "/${var.name}/salesforce/client-secret"
  type  = "SecureString"
  value = var.use_mock_salesforce ? "mock-client-secret" : var.salesforce_client_secret
}

locals {
  instance      = "${var.name}-run${var.run_id}"
  prefix        = "archive/run-${var.run_id}"
  image         = "${aws_ecr_repository.this.repository_url}:${var.image_tag}"
  mock_host     = "mock-salesforce.${aws_service_discovery_private_dns_namespace.this.name}"
  token_url     = var.use_mock_salesforce ? "http://${local.mock_host}:8080" : var.salesforce_token_url
  client_id     = var.use_mock_salesforce ? "mock-client-id" : var.salesforce_client_id
  cache_host    = aws_elasticache_replication_group.this.primary_endpoint_address
  pubsub_config = var.use_mock_salesforce ? "  pubsubEndpoint: ${local.mock_host}:7011\n  pubsubInsecure: true\n" : ""

  stream_config = <<-YAML
    version: "3.0"
    logLevel: info
    env: ${var.name}-run${var.run_id}
    metricsAddr: ":9090"
    eventStream:
      instanceName: ${local.instance}
      auth:
        tokenUrl: ${local.token_url}
        clientCred:
          clientId: ${local.client_id}
          clientSecret: $SF_CLIENT_SECRET
      cache:
        redis:
          host: ${local.cache_host}
          port: 6379
          tls:
            enabled: true
          iamAuth:
            enabled: true
            cacheName: ${aws_elasticache_replication_group.this.id}
            userId: ${aws_elasticache_user.iam.user_id}
            region: ${var.region}
    ${local.pubsub_config}
      initialReplay: EARLIEST
      appetite: 100
      leaseTtlSeconds: 30
      shutdownFlushTimeoutSeconds: 90
      batch:
        maxEvents: 2000
        maxAgeSeconds: 10
      topics: ${jsonencode(var.stream_topics)}
    archive:
      s3:
        bucket: ${aws_s3_bucket.archive.bucket}
        prefix: ${local.prefix}
        region: ${var.region}
        kmsKeyId: ${aws_kms_key.archive.arn}
  YAML

  eventlog_config = <<-YAML
    version: "3.0"
    logLevel: info
    env: ${var.name}-run${var.run_id}
    metricsAddr: ":9090"
    eventLog:
      instanceName: ${local.instance}
      apiVer: "64.0"
      requestTimeout: 30
      pollIntervalSeconds: ${var.use_mock_salesforce ? 30 : 300}
      auth:
        tokenUrl: ${local.token_url}
        clientCred:
          clientId: ${local.client_id}
          clientSecret: $SF_CLIENT_SECRET
      cache:
        redis:
          host: ${local.cache_host}
          port: 6379
          username: ${aws_elasticache_user.password.user_name}
          password: $REDIS_PASSWORD
          expireDays: 7
          tls:
            enabled: true
      initialTimeInterval:
        hours: 24
      customQueries:
        - soql:
            select: [Id, Action, Section, CreatedDate, CreatedById]
            from: SetupAuditTrail
          timestamp: CreatedDate
          apiName: rest
    archive:
      s3:
        bucket: ${aws_s3_bucket.archive.bucket}
        prefix: ${local.prefix}
        region: ${var.region}
        kmsKeyId: ${aws_kms_key.archive.arn}
  YAML

  log_config = {
    logDriver = "awslogs"
    options = {
      awslogs-group         = aws_cloudwatch_log_group.this.name
      awslogs-region        = var.region
      awslogs-stream-prefix = "ecs"
    }
  }
}

resource "aws_ecs_task_definition" "mock" {
  count                    = var.use_mock_salesforce ? 1 : 0
  family                   = "${var.name}-mock-salesforce"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.execution.arn
  runtime_platform {
    cpu_architecture        = "X86_64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name             = "mock-salesforce"
    image            = local.image
    essential        = true
    command          = ["mock-salesforce", "-base-url", "http://${local.mock_host}:8080", "-keepalive", "10s"]
    portMappings     = [{ containerPort = 8080 }, { containerPort = 7011 }]
    logConfiguration = local.log_config
  }])
}

resource "aws_ecs_task_definition" "stream" {
  family                   = "${var.name}-stream"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.collector.arn
  runtime_platform {
    cpu_architecture        = "X86_64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name             = "stream-collector"
    image            = local.image
    essential        = true
    entryPoint       = ["/bin/sh", "-c"]
    command          = ["printf '%s' \"$CONFIG_YAML\" > /tmp/config.yml && exec sf-archive-stream -config /tmp/config.yml"]
    stopTimeout      = 120 # Fargate maximum; above shutdownFlushTimeoutSeconds (90)
    environment      = [{ name = "CONFIG_YAML", value = local.stream_config }]
    secrets          = [{ name = "SF_CLIENT_SECRET", valueFrom = aws_ssm_parameter.sf_client_secret.arn }]
    portMappings     = [{ containerPort = 9090 }]
    logConfiguration = local.log_config
  }])
}

resource "aws_ecs_task_definition" "eventlog" {
  family                   = "${var.name}-eventlog"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.collector.arn
  runtime_platform {
    cpu_architecture        = "X86_64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name        = "eventlog-collector"
    image       = local.image
    essential   = true
    entryPoint  = ["/bin/sh", "-c"]
    command     = ["printf '%s' \"$CONFIG_YAML\" > /tmp/config.yml && exec sf-archive-eventlog -config /tmp/config.yml"]
    stopTimeout = 120 # Fargate maximum; above shutdownFlushTimeoutSeconds (90)
    environment = [{ name = "CONFIG_YAML", value = local.eventlog_config }]
    secrets = [
      { name = "SF_CLIENT_SECRET", valueFrom = aws_ssm_parameter.sf_client_secret.arn },
      { name = "REDIS_PASSWORD", valueFrom = aws_ssm_parameter.cache_password.arn },
    ]
    portMappings     = [{ containerPort = 9090 }]
    logConfiguration = local.log_config
  }])
}

# Run on demand: aws ecs run-task ... --overrides to pass -ledger when using the mock.
resource "aws_ecs_task_definition" "verify" {
  family                   = "${var.name}-verify"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.verify.arn
  runtime_platform {
    cpu_architecture        = "X86_64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name      = "verify"
    image     = local.image
    essential = true
    command = concat(
      ["sf-archive-verify", "-bucket", aws_s3_bucket.archive.bucket, "-prefix", local.prefix, "-region", var.region],
      var.use_mock_salesforce ? ["-ledger", "http://${local.mock_host}:8080/admin/ledger"] : []
    )
    logConfiguration = local.log_config
  }])
}

locals {
  network = {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.tasks.id]
    assign_public_ip = true
  }
}

resource "aws_ecs_service" "mock" {
  count           = var.use_mock_salesforce ? 1 : 0
  name            = "mock-salesforce"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.mock[0].arn
  desired_count   = 1
  launch_type     = "FARGATE"
  # Never run two mocks at once: Cloud Map would return both and collectors
  # could read from a mock whose ledger is about to disappear.
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  network_configuration {
    subnets          = local.network.subnets
    security_groups  = local.network.security_groups
    assign_public_ip = true
  }
  service_registries {
    registry_arn = aws_service_discovery_service.mock[0].arn
  }
}

# Stop the old task before starting a new one (Recreate). The checkpoint lease
# still protects against overlap if ECS ever runs two tasks.
resource "aws_ecs_service" "stream" {
  name                               = "stream-collector"
  cluster                            = aws_ecs_cluster.this.id
  task_definition                    = aws_ecs_task_definition.stream.arn
  desired_count                      = 1
  launch_type                        = "FARGATE"
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  network_configuration {
    subnets          = local.network.subnets
    security_groups  = local.network.security_groups
    assign_public_ip = true
  }
  depends_on = [aws_ecs_service.mock]
}

resource "aws_ecs_service" "eventlog" {
  name                               = "eventlog-collector"
  cluster                            = aws_ecs_cluster.this.id
  task_definition                    = aws_ecs_task_definition.eventlog.arn
  desired_count                      = 1
  launch_type                        = "FARGATE"
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  network_configuration {
    subnets          = local.network.subnets
    security_groups  = local.network.security_groups
    assign_public_ip = true
  }
  depends_on = [aws_ecs_service.mock]
}
