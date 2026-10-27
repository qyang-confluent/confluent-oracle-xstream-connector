variable "environment_name" {
  description = "Display name of an existing Confluent Cloud environment"
  type        = string
  default     = "Qi"
}

variable "cluster_name" {
  type    = string
  default = "poc"
}

variable "cloud" {
  type    = string
  default = "AWS"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "availability" {
  type    = string
  default = "SINGLE_ZONE"
}

variable "cku" {
  type    = number
  default = 1
}

variable "connector_name" {
  type    = string
  default = "rac-xstream-cc-xxx"
}

variable "topic_prefix" {
  type    = string
  default = "ORCL"
}

variable "table_include_list" {
  default     = "HR.EMP,HR.EMPLOYEES"
  description = "Comma-separated SCHEMA.TABLE list"
  type        = string
}

variable "snapshot_mode" {
  type    = string
  default = "no_data"
}

variable "schema_history_topic" {
  type    = string
  default = "rac-orcl-schema-changes.cflt"
}

variable "confluent_topic" {
  description = "Pre-created topic used by the connector for license/metadata"
  type        = string
  default     = "oracle-xstream-connector"
}

variable "confluent_license" {
  description = "Connector license; leave empty to use the 30-day trial"
  type        = string
  default     = ""
  sensitive   = true
}

variable "oracle" {
  description = "Oracle connection. Set `downstream` for downstream-capture topology."
  sensitive   = true
  type = object({
    hostname        = optional(string, "rac-scan")
    port            = optional(number, 1521)
    user            = optional(string, "CFLTUSER")
    password        = string
    dbname          = optional(string, "ORCL")
    service_name    = optional(string, "SYS\\$CFLTADMIN.Q\\$_XOUT_229.ORCL")
    out_server_name = optional(string, "XOUT")
    downstream = optional(object({
      hostname     = string
      port         = optional(number, 1521)
      dbname       = string
      service_name = string
    }))
  })
}

variable "plugin_path" {
  description = "Connect worker plugin.path"
  type        = string
  default     = "/usr/share/java,/home/qyang/confluent-7.9.0/share/confluent-hub-components,/home/qyang/confluent-connectors"
}

variable "offset_file" {
  type    = string
  default = "/tmp/connect.offsets"
}

variable "output_dir" {
  description = "Where the generated worker/connector configs are written"
  type        = string
  default     = "generated"
}
