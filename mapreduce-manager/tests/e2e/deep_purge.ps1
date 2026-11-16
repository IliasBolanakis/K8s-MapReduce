<#
.SYNOPSIS
    MapReduce Framework - Deep Purge & Cluster Reset (Minikube K8s)
.DESCRIPTION
    Executes a hard wipe of the distributed system's state inside Minikube:
    destroys active Kubernetes worker Jobs/Pods, flushes RabbitMQ queues,
    wipes Redis cache, and truncates the Postgres jobs table.
#>

$ErrorActionPreference = "Continue"
$K8S_NAMESPACE = "default"

Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host " INITIATING DEEP PURGE SEQUENCE (K8s Minikube)" -ForegroundColor Cyan
Write-Host "===============================================================================" -ForegroundColor Cyan

# =============================================================================
# 1. KUBERNETES CLEANUP
# =============================================================================
Write-Host "`n[INFO] Phase 1: Destroying Kubernetes Worker Jobs & Pods..." -ForegroundColor Cyan
kubectl delete jobs --all --namespace $K8S_NAMESPACE --wait=false 2>$null
kubectl delete pods -l app=worker-node --namespace $K8S_NAMESPACE --force --grace-period=0 2>$null
Write-Host "[SUCCESS] K8s worker jobs/pods wiped." -ForegroundColor Green

# =============================================================================
# 2. RABBITMQ MESSAGE FLUSH
# =============================================================================
Write-Host "`n[INFO] Phase 2: Purging RabbitMQ Queues..." -ForegroundColor Cyan
$rmqPod = (kubectl get pods -n $K8S_NAMESPACE -l app=rabbitmq -o jsonpath='{.items[0].metadata.name}' 2>$null).Trim()
if ($rmqPod) {
    foreach ($q in @("job_events_queue", "orchestration_events_queue", "dead_letter_queue")) {
        kubectl exec -n $K8S_NAMESPACE $rmqPod -- rabbitmqctl purge_queue $q 2>$null | Out-Null
    }
}
Write-Host "[SUCCESS] RabbitMQ event queues flushed." -ForegroundColor Green

# =============================================================================
# 3. REDIS STATE WIPE
# =============================================================================
Write-Host "`n[INFO] Phase 3: Wiping Redis State Memory..." -ForegroundColor Cyan
$redisPod = (kubectl get pods -n $K8S_NAMESPACE -l app=redis -o jsonpath='{.items[0].metadata.name}' 2>$null).Trim()
if ($redisPod) {
    kubectl exec -n $K8S_NAMESPACE $redisPod -- redis-cli FLUSHALL | Out-Null
}
Write-Host "[SUCCESS] Redis state wiped." -ForegroundColor Green

# =============================================================================
# 4. POSTGRES DATABASE RESET
# =============================================================================
Write-Host "`n[INFO] Phase 4: Truncating Job History in Postgres..." -ForegroundColor Cyan
$pgPod = (kubectl get pods -n $K8S_NAMESPACE -l app=postgres -o jsonpath='{.items[0].metadata.name}' 2>$null).Trim()
if ($pgPod) {
    kubectl exec -n $K8S_NAMESPACE $pgPod -- psql -U postgres -d dds_db -c "DELETE FROM public.jobs;" | Out-Null
}
Write-Host "[SUCCESS] Job history truncated." -ForegroundColor Green

Write-Host "`n===============================================================================" -ForegroundColor Cyan
Write-Host " DEEP PURGE COMPLETE. Cluster is ready for new jobs." -ForegroundColor Green
Write-Host "===============================================================================" -ForegroundColor Cyan
