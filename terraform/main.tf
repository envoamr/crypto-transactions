terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.21.0"
    }
  }
}

provider "google" {
  project = var.projectID
  region  = var.region
}

##############################
# 1. Create the Service Account that will hold GCP permissions that Kestra needs
##############################

resource "google_service_account" "kestra_sa_gcs_bq" {
  account_id = "kestra-sa-gcs-bq"
  project    = var.projectID
}

##############################
# 2. Assign IAM Roles to the Service Account for GCS and BigQuery
##############################

resource "google_project_iam_member" "kestra_sa_gcs_permissions" {
  project = var.projectID
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.kestra_sa_gcs_bq.email}"
}

resource "google_project_iam_member" "kestra_sa_bq_permissions" {
  project = var.projectID
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.kestra_sa_gcs_bq.email}"
}

resource "google_project_iam_member" "bigquery_job_permission" {
  project = var.projectID
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.kestra_sa_gcs_bq.email}"
}

resource "google_project_iam_member" "bigquery_reader_permission" {
  project = var.projectID
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.kestra_sa_gcs_bq.email}"
}

resource "google_project_iam_member" "kestra_sa_bq_permissions_public" {
  project = "bigquery-public-data"
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.kestra_sa_gcs_bq.email}"
}

resource "google_project_iam_member" "dataproc_serverless_editor_permission" {
  project = var.projectID
  role    = "roles/dataproc.serverlessEditor"
  member  = "serviceAccount:${google_service_account.kestra_sa_gcs_bq.email}"
}

resource "google_project_iam_member" "dataproc_service_agent_permission" {
  project = var.projectID
  role    = "roles/dataproc.serviceAgent"
  member  = "serviceAccount:${google_service_account.kestra_sa_gcs_bq.email}"
}

##############################
# 3. Create a Firewall Rule for Kestra UI to connect from your local machine
##############################

resource "google_compute_firewall" "kestra_ui_port" {
  name    = "kestra-ui-port"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  direction     = "INGRESS"
  priority      = 1000
  source_ranges = ["<YOUR IP ADDRESS>/32"] # Replace with your actual public IP
  target_tags   = ["kestra-vm"]
}

#############################
# 4. VM Compute Instance for KESTRA
#############################

resource "google_compute_instance" "instance_kestra" {
  name         = "${var.projectID}-kestra-vm"
  machine_type = "e2-medium"
  zone         = var.zone

  boot_disk {
    auto_delete = true
    device_name = "${var.projectID}-kestra-bootdisk"
    mode        = "READ_WRITE"

    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20250305"
      size  = 20
      type  = "pd-balanced"
    }
  }

  network_interface {
    access_config {
      network_tier = "PREMIUM"
    }
    stack_type = "IPV4_ONLY"
    subnetwork = "projects/${var.projectID}/regions/${var.region}/subnetworks/default"
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
    provisioning_model  = "STANDARD"
  }

  service_account {
    email  = google_service_account.kestra_sa_gcs_bq.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_secure_boot          = false
    enable_vtpm                 = true
  }

  metadata = {
    enable-osconfig = "TRUE"
  }

  labels = {
    "goog-ec-src"           = "vm_add-tf"
    "goog-ops-agent-policy" = "v2-x86-template-1-4-0"
  }

  enable_display = false

  tags = ["https-server", "kestra-vm"]
}

module "ops_agent_policy" {
  source        = "github.com/terraform-google-modules/terraform-google-cloud-operations/modules/ops-agent-policy"
  project       = var.projectID
  zone          = var.zone
  assignment_id = "goog-ops-agent-v2-x86-template-1-4-0-${var.zone}"

  agents_rule = {
    package_state = "installed"
    version       = "latest"
  }

  instance_filter = {
    all = false
    inclusion_labels = [{
      labels = {
        "goog-ops-agent-policy" = "v2-x86-template-1-4-0"
      }
    }]
  }
}

#############################
# 5. GCS Bucket
#############################

resource "google_storage_bucket" "my_bucket" {
  name          = "${var.projectID}-bucket-lake"
  location      = "US"
  storage_class = "STANDARD"

  force_destroy = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }
}

resource "google_storage_bucket_object" "pyspark_script" {
  name   = "scripts/pyspark-transform-crypto.py"
  bucket = google_storage_bucket.my_bucket.name
  source = "../src/scripts/pyspark-transform-crypto.py"
}

resource "google_storage_bucket_object" "jar_pyspark" {
  name   = "jars/spark-3.5-bigquery-0.42.1.jar"
  bucket = google_storage_bucket.my_bucket.name
  source = "../src/jars/spark-3.5-bigquery-0.42.1.jar"
}

resource "google_storage_bucket_object" "jar_gcs" {
  name   = "jars/gcs-connector-hadoop3-latest.jar"
  bucket = google_storage_bucket.my_bucket.name
  source = "../src/jars/gcs-connector-hadoop3-latest.jar"
}

#############################
# 6. BIG QUERY
#############################

resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id  = "de_zoomcamp_crypto_dataset"
  description = "dataset description"
  location    = "US"

  labels = {
    billing_group = "accounting",
    pii           = "sensitive"
  }
}

resource "google_bigquery_job" "create_tables" {
  job_id = "create-tables-job-${uuid()}"

  query {
    query = <<-EOT
      CREATE TABLE \`${var.projectID}.${google_bigquery_dataset.bq_dataset.dataset_id}.fact_transactions\` (
        cryptocurrency STRING,
        transaction_id STRING,
        block_timestamp TIMESTAMP,
        block_timestamp_15m TIMESTAMP,
        block_timestamp_month DATE,
        input_value FLOAT64,
        output_value FLOAT64,
        fee FLOAT64,
        input_value_usd FLOAT64,
        output_value_usd FLOAT64,
        fee_usd FLOAT64
      )
      PARTITION BY block_timestamp_month
      CLUSTER BY cryptocurrency;

      CREATE TABLE \`${var.projectID}.${google_bigquery_dataset.bq_dataset.dataset_id}.dim_market_price\` (
        cryptocurrency STRING,
        open_time TIMESTAMP,
        open_price FLOAT64,
        high_price FLOAT64,
        low_price FLOAT64,
        close_price FLOAT64,
        volume FLOAT64,
        number_of_trades INT64,
        timestamp_month DATE
      )
      PARTITION BY timestamp_month
      CLUSTER BY cryptocurrency;
    EOT

    use_legacy_sql = false
  }
}