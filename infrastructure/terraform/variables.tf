variable "environment" {
  type        = string
  default     = "development"
  description = "Target deployment environment (development, staging, production)"
}

variable "region" {
  type        = string
  default     = "ap-south-1"
  description = "Target cloud region"
}

variable "cluster_node_count" {
  type        = number
  default     = 3
  description = "Number of Kubernetes worker nodes"
}

variable "instance_type" {
  type        = string
  default     = "t3.medium"
  description = "Cloud compute instance type"
}
