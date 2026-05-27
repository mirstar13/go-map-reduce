$ProjectID = gcloud config get-value project
$Region = "us-central1"
$ClusterName = "mapreduce-cluster"

Write-Host "Using Project: $ProjectID"
Write-Host "Creating GKE Autopilot cluster: $ClusterName in $Region..."

gcloud container clusters create-auto $ClusterName `
    --region $Region `
    --project $ProjectID

Write-Host "Cluster created. Fetching credentials..."
gcloud container clusters get-credentials $ClusterName --region $Region

Write-Host "Registering Google Artifact Registry for images..."
try {
    gcloud artifacts repositories create mapreduce-repo `
        --repository-format=docker `
        --location=$Region `
        --description="Docker repository for MapReduce images"
} catch {
    Write-Host "Repository may already exist, continuing..."
}

Write-Host "Setup complete. You can now run .\deploy-gcp.ps1"
