terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.11.0"
    }
  }
}


provider "google" {
  project = "dtc-de-course-412219"
  region  = "us-central1"
  zone    = "us-central1-c"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "dtc-de-zoomcamp-2024-demo-bucket"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
