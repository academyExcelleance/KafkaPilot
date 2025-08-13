terraform {
  required_version = ">= 1.2.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      # pin to a modern provider series; adjust as needed
      version = "~> 6.14"
    }
  }
}
