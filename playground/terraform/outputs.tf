output "bootstrap_servers" {
  value = local.bootstrap
}

output "schema_registry_url" {
  value = local.sr_url
}

output "service_account_id" {
  value = confluent_service_account.connector.id
}

output "generated_files" {
  value = [
    local_sensitive_file.worker_config.filename,
    local_sensitive_file.connector_config.filename,
  ]
}
