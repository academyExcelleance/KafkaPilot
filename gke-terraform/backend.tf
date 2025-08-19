terraform {
  backend "gcs" {
    bucket = "kafkapilot-state-bucket"
    prefix = "gke/tf-state"
  }
}
