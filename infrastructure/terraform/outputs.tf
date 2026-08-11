output "environment" {
  value       = var.environment
  description = "Target deployment environment"
}

output "kafka_broker_endpoint" {
  value       = "sentinel-kafka:9092"
  description = "Internal Kafka broker endpoint"
}

output "cluster_node_count" {
  value       = var.cluster_node_count
  description = "Kubernetes worker node count"
}
