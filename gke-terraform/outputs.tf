output "cluster_name" {
  value = google_container_cluster.primary.name
}

output "endpoint" {
  value = google_container_cluster.primary.endpoint
}

output "kubeconfig" {
  description = "Kubeconfig entry (base64). You can decode/use it to configure kubectl."
  value       = google_container_cluster.primary.master_auth[0].cluster_ca_certificate
}
