#!/bin/bash
# enable_apis.sh — Enable all GCP APIs required for the pipeline
# Run once after creating your GCP project

set -e

echo "🔧 Enabling required GCP APIs..."

APIS=(
  "storage.googleapis.com"
  "bigquery.googleapis.com"
  "dataflow.googleapis.com"
  "pubsub.googleapis.com"
  "composer.googleapis.com"
  "cloudbuild.googleapis.com"
  "iam.googleapis.com"
  "cloudresourcemanager.googleapis.com"
  "monitoring.googleapis.com"
  "logging.googleapis.com"
  "iamcredentials.googleapis.com"
  "serviceusage.googleapis.com"
)

for API in "${APIS[@]}"; do
  echo "  Enabling $API..."
  gcloud services enable "$API" --quiet
done

echo "✅ All APIs enabled."
echo ""
echo "Verify with: gcloud services list --enabled"
