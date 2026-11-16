<#
.SYNOPSIS
    MapReduce Framework - Multi-Chunk Large File E2E Runner (Minikube K8s)
.DESCRIPTION
    Resets cluster state, uploads a >128MB multi-chunk dataset to MinIO, and runs
    a WordCount+Combiner job with 8 Reducers against the live Minikube cluster.
#>

$ErrorActionPreference = "Stop"

$K8S_NAMESPACE  = "default"
$USER_ID        = "22222222-2222-2222-2222-222222222222"

$E2E_DIR        = $PSScriptRoot
$MANAGER_ROOT   = (Resolve-Path (Join-Path $E2E_DIR "..\..")).Path
$PYTHON_EXE     = Join-Path $MANAGER_ROOT ".venv\Scripts\python.exe"

Write-Host "`n[INFO] Purging previous cluster state and ensuring seeded infrastructure..." -ForegroundColor Cyan
& (Join-Path $E2E_DIR "deep_purge.ps1")
& (Join-Path $E2E_DIR "start_infra_and_seed.ps1")

kubectl rollout status deployment/manager-api -n $K8S_NAMESPACE --timeout=180s
kubectl rollout status daemonset/external-shuffle-service -n $K8S_NAMESPACE --timeout=180s

$pfMinio = Start-Process -FilePath "kubectl" -ArgumentList "port-forward", "-n", $K8S_NAMESPACE, "svc/minio-service", "19000:9000" -PassThru -WindowStyle Hidden
$pfMgr   = Start-Process -FilePath "kubectl" -ArgumentList "port-forward", "-n", $K8S_NAMESPACE, "svc/manager-service", "18000:8000" -PassThru -WindowStyle Hidden

try {
    Start-Sleep -Seconds 3
    $fileName = "wordcount_multichunk_200mb.txt"
    $tmpFile = Join-Path $env:TEMP $fileName

    Write-Host "`n[INFO] Generating >128MB multi-chunk dataset and uploading to MinIO..." -ForegroundColor Cyan
    $fileSize = & $PYTHON_EXE -c @"
import os
from minio import Minio

line = b'alpha beta gamma alpha delta epsilon zeta eta theta iota\n'
target_bytes = 150 * 1024 * 1024
path = r'$tmpFile'
written = 0
block = line * 1024
with open(path, 'wb') as f:
    while written < target_bytes:
        f.write(block)
        written += len(block)

client = Minio('127.0.0.1:19000', access_key='minioadmin', secret_key='minioadmin', secure=False)
client.fput_object('data', '$USER_ID/$fileName', path)
os.remove(path)
print(written)
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
        num_reducers      = 8
    } | ConvertTo-Json

    $headers = @{ "Idempotency-Key" = $jobId }
    $resp = Invoke-RestMethod -Uri "http://127.0.0.1:18000/internal/schedule" -Method Post -Headers $headers -Body $body -ContentType "application/json"
    Write-Host "[SUCCESS] Large multi-chunk Job $jobId submitted ($($resp.spawned_workers) map workers, 8 reducers)" -ForegroundColor Green

    $status = "RUNNING"
    $deadline = (Get-Date).AddMinutes(10)
    while ((Get-Date) -lt $deadline) {
        $jobState = Invoke-RestMethod -Uri "http://127.0.0.1:18000/internal/jobs/$jobId/status" -Method Get
        $status = $jobState.status
        $podCount = (kubectl get pods -n $K8S_NAMESPACE -l job_id=$jobId --no-headers 2>$null | Measure-Object).Count
        Write-Host ("  [Status: {0} | Map: {1} | Reduce: {2} | Active Pods: {3}]" -f $status, $jobState.completed_map_chunks, $jobState.completed_reduce_chunks, $podCount)
        if ($status -eq "COMPLETED" -or $status -eq "FAILED") { break }
        Start-Sleep -Seconds 3
    }

    if ($status -ne "COMPLETED") {
        throw "Large multi-chunk job $jobId ended with status $status"
    }
    Write-Host "`n[FINISH] Multi-Chunk Large File E2E job COMPLETED!" -ForegroundColor Green
} finally {
    if ($pfMinio -and -not $pfMinio.HasExited) { Stop-Process -Id $pfMinio.Id -Force }
    if ($pfMgr -and -not $pfMgr.HasExited) { Stop-Process -Id $pfMgr.Id -Force }
}
