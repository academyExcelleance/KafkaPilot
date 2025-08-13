terraform {
  backend "gcs" {
    bucket = "mcp-server-terraform-state-bucket"
    prefix = "gke/tf-state"
  }
}
