terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.28.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

# Cloud API key/secret come from the environment:
#   source ~/vault/ccloud/dsp-cc.env   # exports CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET
provider "confluent" {}

# ---------------------------------------------------------------------------
# Confluent Cloud: existing environment, new dedicated cluster
# ---------------------------------------------------------------------------

data "confluent_environment" "env" {
  display_name = var.environment_name
}

# Stream Governance must already be enabled on the environment.
data "confluent_schema_registry_cluster" "sr" {
  environment {
    id = data.confluent_environment.env.id
  }
}

resource "confluent_kafka_cluster" "dedicated" {
  display_name = var.cluster_name
  availability = var.availability
  cloud        = var.cloud
  region       = var.region
  dedicated {
    cku = var.cku
  }
  environment {
    id = data.confluent_environment.env.id
  }
}

# ---------------------------------------------------------------------------
# Service account used by the connector + Kafka API key
# ---------------------------------------------------------------------------

resource "confluent_service_account" "connector" {
  display_name = "${var.cluster_name}-xstream-connector"
  description  = "Service account for the Oracle XStream CDC source connector"
}

# PoC-level access: the connector creates topics (data, schema history) and writes to them.
resource "confluent_role_binding" "connector_cluster_admin" {
  principal   = "User:${confluent_service_account.connector.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.dedicated.rbac_crn
}

resource "confluent_api_key" "kafka" {
  display_name = "xstream-connector-kafka-api-key"
  owner {
    id          = confluent_service_account.connector.id
    api_version = confluent_service_account.connector.api_version
    kind        = confluent_service_account.connector.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.dedicated.id
    api_version = confluent_kafka_cluster.dedicated.api_version
    kind        = confluent_kafka_cluster.dedicated.kind
    environment {
      id = data.confluent_environment.env.id
    }
  }
  depends_on = [confluent_role_binding.connector_cluster_admin]
}

# Connector license topic (`confluent.topic`) must be pre-created.
resource "confluent_kafka_topic" "confluent_topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.dedicated.id
  }
  topic_name       = var.confluent_topic
  rest_endpoint    = confluent_kafka_cluster.dedicated.rest_endpoint
  partitions_count = 1
  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}

# ---------------------------------------------------------------------------
# Schema Registry API key for the same service account
# ---------------------------------------------------------------------------

resource "confluent_role_binding" "connector_sr" {
  principal   = "User:${confluent_service_account.connector.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.sr.resource_name}/subject=*"
}

resource "confluent_api_key" "sr" {
  display_name = "xstream-connector-sr-api-key"
  owner {
    id          = confluent_service_account.connector.id
    api_version = confluent_service_account.connector.api_version
    kind        = confluent_service_account.connector.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind
    environment {
      id = data.confluent_environment.env.id
    }
  }
  depends_on = [confluent_role_binding.connector_sr]
}

# ---------------------------------------------------------------------------
# Generated standalone Connect worker + connector configs
# ---------------------------------------------------------------------------

locals {
  # bootstrap_endpoint is "SASL_SSL://host:9092"
  bootstrap = trimprefix(confluent_kafka_cluster.dedicated.bootstrap_endpoint, "SASL_SSL://")
  sr_url    = data.confluent_schema_registry_cluster.sr.rest_endpoint
  jaas = format(
    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
    confluent_api_key.kafka.id, confluent_api_key.kafka.secret,
  )

  # Client security props; prefixed per client for the worker and the connector's internal clients.
  client_security = {
    "security.protocol"                     = "SASL_SSL"
    "sasl.mechanism"                        = "PLAIN"
    "sasl.jaas.config"                      = local.jaas
    "request.timeout.ms"                    = "20000"
    "retry.backoff.ms"                      = "500"
    "ssl.endpoint.identification.algorithm" = "https"
  }

  worker = merge(
    {
      "bootstrap.servers"              = local.bootstrap
      "key.converter"                  = "org.apache.kafka.connect.storage.StringConverter"
      "value.converter"                = "org.apache.kafka.connect.json.JsonConverter"
      "value.converter.schemas.enable" = "false"
      "offset.flush.interval.ms"       = "10000"
      "offset.storage.file.filename"   = var.offset_file
      "plugin.path"                    = var.plugin_path
    },
    local.client_security,
    { for k, v in local.client_security : "producer.${k}" => v },
    { for k, v in local.client_security : "consumer.${k}" => v },
    { for k, v in local.client_security : "admin.${k}" => v },
    { "producer.bootstrap.servers" = local.bootstrap, "consumer.bootstrap.servers" = local.bootstrap },
  )

  connector = merge(
    {
      "name"            = var.connector_name
      "connector.class" = "io.confluent.connect.oracle.xstream.cdc.OracleXStreamSourceConnector"
      "tasks.max"       = "1"

      "database.hostname"        = var.oracle.hostname
      "database.port"            = tostring(var.oracle.port)
      "database.user"            = var.oracle.user
      "database.password"        = var.oracle.password
      "database.dbname"          = var.oracle.dbname
      "database.service.name"    = var.oracle.service_name
      "database.out.server.name" = var.oracle.out_server_name

      "topic.prefix"          = var.topic_prefix
      "table.include.list"    = var.table_include_list
      "snapshot.mode"         = var.snapshot_mode
      "decimal.handling.mode" = "string"

      "key.converter"                                        = "io.confluent.connect.avro.AvroConverter"
      "key.converter.schema.registry.url"                    = local.sr_url
      "key.converter.basic.auth.credentials.source"          = "USER_INFO"
      "key.converter.schema.registry.basic.auth.user.info"   = "${confluent_api_key.sr.id}:${confluent_api_key.sr.secret}"
      "value.converter"                                      = "io.confluent.connect.avro.AvroConverter"
      "value.converter.schema.registry.url"                  = local.sr_url
      "value.converter.basic.auth.credentials.source"        = "USER_INFO"
      "value.converter.schema.registry.basic.auth.user.info" = "${confluent_api_key.sr.id}:${confluent_api_key.sr.secret}"

      "schema.history.internal.kafka.topic"                    = var.schema_history_topic
      "schema.history.internal.kafka.bootstrap.servers"        = local.bootstrap
      "schema.history.internal.skip.unparseable.ddl"           = "true"
      "schema.history.internal.store.only.captured.tables.ddl" = "true"

      "topic.creation.default.partitions"         = "1"
      "topic.creation.default.replication.factor" = "3"

      "confluent.topic"                   = confluent_kafka_topic.confluent_topic.topic_name
      "confluent.topic.bootstrap.servers" = local.bootstrap
    },
    { for k, v in local.client_security : "schema.history.internal.producer.${k}" => v },
    { for k, v in local.client_security : "schema.history.internal.consumer.${k}" => v },
    { for k, v in local.client_security : "confluent.topic.${k}" => v if k != "request.timeout.ms" && k != "retry.backoff.ms" },
    var.confluent_license == "" ? {} : { "confluent.license" = var.confluent_license },
    var.oracle.downstream == null ? {} : {
      "downstream.database.hostname"     = var.oracle.downstream.hostname
      "downstream.database.port"         = tostring(var.oracle.downstream.port)
      "downstream.database.dbname"       = var.oracle.downstream.dbname
      "downstream.database.service.name" = var.oracle.downstream.service_name
    },
  )
}

resource "local_sensitive_file" "worker_config" {
  filename        = "${var.output_dir}/cp-local-worker.config"
  file_permission = "0600"
  content         = "${join("\n", [for k in sort(keys(local.worker)) : "${k}=${local.worker[k]}"])}\n"
}

resource "local_sensitive_file" "connector_config" {
  filename        = "${var.output_dir}/oracle-xstream-rac.config"
  file_permission = "0600"
  content         = "${jsonencode(local.connector)}\n"
}
