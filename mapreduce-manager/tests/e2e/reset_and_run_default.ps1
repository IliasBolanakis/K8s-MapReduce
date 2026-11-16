<#
.SYNOPSIS
    MapReduce Framework - Monikube Cluster Deploy & Default WordCount E2E Runner
.DESCRIPTION
    Builds all 3 component images inside Minikube Docker daemon, deploys infra +
    manager-api + ESS DaemonSet, seeds MinIO with compiled WordCount classes and a
    generated dataset, submits a WordCount+Combiner job via kubectl port-forward,
    and monitors execution until COMPLETED.
#>

$ErrorActionPreference = "Stop"

# =============================================================================
# 1. CONFIGURATION (MONOREPO PATHS)
# =============================================================================
$K8S_NAMESPACE  = "default"
$USER_ID        = "22222222-2222-2222-2222-222222222222"

$E2E_DIR        = $PSScriptRoot
$MANAGER_ROOT   = (Resolve-Path (Join-Path $E2E_DIR "..\..")).Path
$MONOREPO_ROOT  = (Resolve-Path (Join-Path $MANAGER_ROOT "..")).Path
$NODE_ROOT      = Join-Path $MONOREPO_ROOT "mapreduce-node"
$ESS_DIR        = Join-Path $MONOREPO_ROOT "mapreduce-ess"
$K8S_DIR        = Join-Path $MANAGER_ROOT "app\k8s"
$PYTHON_EXE     = Join-Path $MANAGER_ROOT ".venv\Scripts\python.exe"

# =============================================================================
# 2. CLEANUP & BUILD IMAGES IN MINIKUBE
# =============================================================================
Write-Host "`n[INFO] Phase 1: Purging previous state and building images in Minikube..." -ForegroundColor Cyan
& (Join-Path $E2E_DIR "deep_purge.ps1")

mvn -f (Join-Path $ESS_DIR "pom.xml") package -DskipTests | Out-Null

& minikube -p minikube docker-env --shell powershell | Invoke-Expression
docker build -q -t mapreduce-ess:v2 $ESS_DIR
docker build -q -t mapreduce-worker:v10 $NODE_ROOT
docker build -q -t mapreduce-manager:v1 $MANAGER_ROOT

# =============================================================================
# 3. DEPLOY INFRASTRUCTURE, MANAGER & ESS
# =============================================================================
Write-Host "`n[INFO] Phase 2: Deploying K8s manifests and waiting for Ready state..." -ForegroundColor Cyan
& (Join-Path $E2E_DIR "start_infra_and_seed.ps1")

kubectl apply -f (Join-Path $K8S_DIR "manager\manager-rbac.yaml") -n $K8S_NAMESPACE
kubectl apply -f (Join-Path $K8S_DIR "manager\manager-service.yaml") -n $K8S_NAMESPACE
kubectl apply -f (Join-Path $K8S_DIR "manager\manager-deployment.yaml") -n $K8S_NAMESPACE
kubectl set env deployment/manager-api -n $K8S_NAMESPACE ENABLE_AUTH=false
kubectl apply -f (Join-Path $K8S_DIR "templates\ess-daemonset.yaml") -n $K8S_NAMESPACE

kubectl rollout status deployment/manager-api -n $K8S_NAMESPACE --timeout=180s
kubectl rollout status daemonset/external-shuffle-service -n $K8S_NAMESPACE --timeout=180s

# =============================================================================
# 4. SEED SAMPLE DATASET & EXECUTE WORDCOUNT JOB
# =============================================================================
Write-Host "`n[INFO] Phase 3: Seeding sample dataset and submitting WordCount job..." -ForegroundColor Cyan
$pfMinio = Start-Process -FilePath "kubectl" -ArgumentList "port-forward", "-n", $K8S_NAMESPACE, "svc/minio-service", "19000:9000" -PassThru -WindowStyle Hidden
$pfMgr   = Start-Process -FilePath "kubectl" -ArgumentList "port-forward", "-n", $K8S_NAMESPACE, "svc/manager-service", "18000:8000" -PassThru -WindowStyle Hidden

try {
    Start-Sleep -Seconds 3
    $fileName = "wordcount_default.txt"
    $fileSize = & $PYTHON_EXE -c @"
import io
from minio import Minio
client = Minio('127.0.0.1:19000', access_key='minioadmin', secret_key='minioadmin', secure=False)
payload = ('alpha beta gamma alpha delta epsilon\n' * 5000).encode('utf-8')
client.put_object('data', '$USER_ID/$fileName', io.BytesIO(payload), len(payload))
print(len(payload))
"@

    $jobId = [guid]::NewGuid().ToString()
    $body = @{
        job_id            = $jobId
        format            = "TEXT"
        input_filename    = $fileName
        file_size         = [int64]$fileSize
        mapper_filename   = "WordCountMapper.class"
        combiner_filename = "WordCountCombiner.class"
        reducer_filename  = "WordCountReducer.class"
        num_reducers      = 2
    } | ConvertTo-Json

    $headers = @{ "Idempotency-Key" = $jobId }
    $resp = Invoke-RestMethod -Uri "http://127.0.0.1:18000/internal/schedule" -Method Post -Headers $headers -Body $body -ContentType "application/json"
    Write-Host "[SUCCESS] Submitted Job $jobId ($($resp.status))" -ForegroundColor Green

    $status = "RUNNING"
    $deadline = (Get-Date).AddMinutes(5)
    while ((Get-Date) -lt $deadline) {
        $jobState = Invoke-RestMethod -Uri "http://127.0.0.1:18000/internal/jobs/$jobId/status" -Method Get
        $status = $jobState.status
        Write-Host ("  [Status: {0} | Map: {1} | Reduce: {2}]" -f $status, $jobState.completed_map_chunks, $jobState.completed_reduce_chunks)
        if ($status -eq "COMPLETED" -or $status -eq "FAILED") { break }
        Start-Sleep -Seconds 2
    }

    if ($status -ne "COMPLETED") {
        throw "WordCount job $jobId ended with status $status"
    }
    Write-Host "`n[FINISH] Default WordCount E2E job COMPLETED!" -ForegroundColor Green
} finally {
    if ($pfMinio -and -not $pfMinio.HasExited) { Stop-Process -Id $pfMinio.Id -Force }
    if ($pfMgr -and -not $pfMgr.HasExited) { Stop-Process -Id $pfMgr.Id -Force }
}
