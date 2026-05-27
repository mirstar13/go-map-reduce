# Google Cloud (GKE) Deployment Guide

This directory contains scripts and configurations for deploying the Distributed MapReduce stack to **Google Kubernetes Engine (GKE)**.

## Prerequisites

1.  **Google Cloud SDK (`gcloud`)**: Installed and authenticated.
2.  **Docker**: Installed and configured with GCP auth:
    ```bash
    gcloud auth configure-docker us-central1-docker.pkg.dev
    ```
3.  **Active GCP Project**: With billing enabled (Free Trial is fine).

## Step 1: Create the Cluster

Run the creation script to provision a **GKE Autopilot** cluster. Autopilot is ideal because it scales workers to zero when no jobs are running, saving costs.

```bash
chmod +x create-cluster.sh
./create-cluster.sh
```

**Windows PowerShell:**
```powershell
.\create-cluster.ps1
```

## Step 2: Build and Deploy

The deployment script automates image building, pushing to Artifact Registry, and applying the manifests using `kustomize`.

```bash
chmod +x deploy-gcp.sh
./deploy-gcp.sh
```

**Windows PowerShell:**
```powershell
.\deploy-gcp.ps1
```

## Step 3: Access the UI

Once deployed, wait for the LoadBalancer to provision a public IP:

```bash
kubectl get svc ui-public -n mapreduce
```

Use the returned IP to configure your CLI:
```bash
mapreduce login --server http://<EXTERNAL-IP> --username alice
```

## Why GKE Autopilot?

*   **Cost Efficiency**: You only pay for the CPU/RAM used by your active pods. Mappers/Reducers that only run for 5 minutes are billed exactly for those 5 minutes.
*   **Auto-Scaling**: GKE will automatically add physical nodes as your MapReduce jobs request more workers.
*   **Security**: Pod-level isolation is enforced by default.
