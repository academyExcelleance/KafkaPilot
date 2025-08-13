provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone

  # Authentication:
  # By default provider uses Application Default Credentials (ADC).
  # You can also set credentials = file("<path-to-service-account-key>.json")
}
