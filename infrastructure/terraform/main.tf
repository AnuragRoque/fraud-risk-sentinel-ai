# SentinelStream Infrastructure as Code — Local & Cloud Architecture

resource "local_file" "k8s_deployment_manifest_copy" {
  content  = <<-EOT
    # SentinelStream Terraform Generated Deployment Spec
    # Environment: ${var.environment}
    # Region: ${var.region}
    # Nodes: ${var.cluster_node_count}
  EOT
  filename = "${path.module}/../../deploy/generated_spec.txt"
}
