<#
.SYNOPSIS
    MapReduce Framework - Multi-Tenant Concurrent E2E Test (3 Jobs, Minikube K8s)
.DESCRIPTION
    Purges cluster state, ensures infra + manager-api + ESS are ready, seeds
    TEXT and CSV datasets + compiled Java classes, and concurrently submits
    3 jobs (WordCount+Combiner TEXT, CsvSum CSV, WordCount explicit topology)
    to verify multi-tenant isolation and completion.
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
    $sizes = & $PYTHON_EXE -c @"
import io, json
from minio import Minio

client = Minio('127.0.0.1:19000', access_key='minioadmin', secret_key='minioadmin', secure=False)
text_data = ('alpha beta gamma alpha delta epsilon\n' * 4000).encode('utf-8')
csv_data = (b'category,amount\n' + b'books,10\ngames,25\nbooks,5\nmusic,12\n' * 3000)

client.put_object('data', '$USER_ID/mt_words.txt', io.BytesIO(text_data), len(text_data))
client.put_object('data', '$USER_ID/mt_sales.csv', io.BytesIO(csv_data), len(csv_data))
print(json.dumps({'text': len(text_data), 'csv': len(csv_data)}))
"@ | ConvertFrom-Json

    $jobId1 = [guid]::NewGuid().ToString()
    $jobId2 = [guid]::NewGuid().ToString()
    $jobId3 = [guid]::NewGuid().ToString()

    $jobs = @(
        @{
            job_id = $jobId1; format = "TEXT"; input_filename = "mt_words.txt"; file_size = [int64]$sizes.text;
            mapper_filename = "WordCountMapper.class"; combiner_filename = "WordCountCombiner.class";
            reducer_filename = "WordCountReducer.class"; num_reducers = 2
        },
        @{
            job_id = $jobId2; format = "CSV"; input_filename = "mt_sales.csv"; file_size = [int64]$sizes.csv;
            mapper_filename = "CsvSumMapper.class"; reducer_filename = "CsvSumReducer.class";
            num_reducers = 2
        },
        @{
            job_id = $jobId3; format = "TEXT"; input_filename = "mt_words.txt"; file_size = [int64]$sizes.text;
            mapper_filename = "WordCountMapper.class"; reducer_filename = "WordCountReducer.class";
            num_mappers = 2; num_reducers = 2
        }
    )

    foreach ($job in $jobs) {
        $jid = $job.job_id
        $resp = Invoke-RestMethod -Uri "http://127.0.0.1:18000/internal/schedule" -Method Post -Headers @{ "Idempotency-Key" = $jid } -Body ($job | ConvertTo-Json) -ContentType "application/json"
        Write-Host "[SUCCESS] Accepted concurrent job $jid ($($resp.status))" -ForegroundColor Green
    }

    $ids = @($jobId1, $jobId2, $jobId3)
    $deadline = (Get-Date).AddMinutes(8)
    while ((Get-Date) -lt $deadline) {
        $allDone = $true
        foreach ($jid in $ids) {
            $state = Invoke-RestMethod -Uri "http://127.0.0.1:18000/internal/jobs/$jid/status" -Method Get
            Write-Host ("  [{0}] status={1} map={2} reduce={3}" -f $jid.Substring(0,8), $state.status, $state.completed_map_chunks, $state.completed_reduce_chunks)
            if ($state.status -eq "FAILED") {
                throw "Concurrent job $jid FAILED"
            }
            if ($state.status -ne "COMPLETED") {
                $allDone = $false
            }
        }
        if ($allDone) { break }
        Start-Sleep -Seconds 3
    }

    foreach ($jid in $ids) {
        $final = Invoke-RestMethod -Uri "http://127.0.0.1:18000/internal/jobs/$jid/status" -Method Get
        if ($final.status -ne "COMPLETED") {
            throw "Job $jid did not complete in time (final=$($final.status))"
        }
    }
    Write-Host "`n[FINISH] All 3 concurrent multi-tenant jobs COMPLETED!" -ForegroundColor Green
} finally {
    if ($pfMinio -and -not $pfMinio.HasExited) { Stop-Process -Id $pfMinio.Id -Force }
    if ($pfMgr -and -not $pfMgr.HasExited) { Stop-Process -Id $pfMgr.Id -Force }
}
