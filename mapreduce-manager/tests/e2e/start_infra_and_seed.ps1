<#
.SYNOPSIS
    Infrastructure Startup & MinIO Seeding Script (Minikube K8s)
.DESCRIPTION
    Deploys the Kubernetes backing infrastructure (PostgreSQL, Redis, MinIO, RabbitMQ),
    compiles the Java E2E fixtures against mapreduce-core, and seeds MinIO buckets
    ('data', 'code', 'results', 'mapreduce-shuffle-overflow') via kubectl port-forward.
#>

$ErrorActionPreference = "Stop"

# =============================================================================
# 1. CONFIGURATION (MONOREPO PATHS)
# =============================================================================
$USER_ID        = "22222222-2222-2222-2222-222222222222"
$K8S_NAMESPACE  = "default"

$E2E_DIR        = $PSScriptRoot
$MANAGER_ROOT   = (Resolve-Path (Join-Path $E2E_DIR "..\..")).Path
$MONOREPO_ROOT  = (Resolve-Path (Join-Path $MANAGER_ROOT "..")).Path
$NODE_ROOT      = Join-Path $MONOREPO_ROOT "mapreduce-node"
$CORE_CLASSES   = Join-Path $NODE_ROOT "mapreduce-core\target\classes"
$K8S_INFRA_DIR  = Join-Path $MANAGER_ROOT "app\k8s\infrastructure"
$FIXTURES_DIR   = Join-Path $E2E_DIR "fixtures"
$PYTHON_EXE     = Join-Path $MANAGER_ROOT ".venv\Scripts\python.exe"

# =============================================================================
# 2. DEPLOY KUBERNETES INFRASTRUCTURE
# =============================================================================
Write-Host "`n[INFO] Applying Kubernetes infrastructure manifests..." -ForegroundColor Cyan
kubectl apply -f (Join-Path $K8S_INFRA_DIR "postgres-configmap.yaml") -n $K8S_NAMESPACE
kubectl apply -f (Join-Path $K8S_INFRA_DIR "postgres.yaml") -n $K8S_NAMESPACE
kubectl apply -f (Join-Path $K8S_INFRA_DIR "redis.yaml") -n $K8S_NAMESPACE
kubectl apply -f (Join-Path $K8S_INFRA_DIR "minio.yaml") -n $K8S_NAMESPACE
kubectl set image deployment/minio -n $K8S_NAMESPACE minio=cgr.dev/chainguard/minio:latest
kubectl apply -f (Join-Path $K8S_INFRA_DIR "rabbitmq.yaml") -n $K8S_NAMESPACE

Write-Host "  -> Waiting for infrastructure pods to become Ready..." -ForegroundColor DarkGray
foreach ($app in "postgres", "redis", "minio", "rabbitmq") {
    kubectl wait --for=condition=ready pod -l app=$app -n $K8S_NAMESPACE --timeout=180s
}

# =============================================================================
# 3. COMPILE JAVA USER CODE FIXTURES
# =============================================================================
Write-Host "`n[INFO] Compiling E2E Java Mapper/Combiner/Reducer fixtures..." -ForegroundColor Cyan
if (-not (Test-Path $CORE_CLASSES)) {
    mvn -f (Join-Path $NODE_ROOT "pom.xml") -pl mapreduce-core compile
}

$tmpRoot = Join-Path $E2E_DIR ".tmp_e2e"
$buildDir = Join-Path $tmpRoot "mr-e2e-classes"
New-Item -ItemType Directory -Force -Path $buildDir | Out-Null
javac --release 17 -cp $CORE_CLASSES -d $buildDir (Join-Path $FIXTURES_DIR "*.java")

# =============================================================================
# 4. SEED MINIO BUCKETS VIA PORT-FORWARD
# =============================================================================
Write-Host "`n[INFO] Seeding MinIO buckets ('data', 'code', 'results', 'mapreduce-shuffle-overflow')..." -ForegroundColor Cyan
$pf = Start-Process -FilePath "kubectl" -ArgumentList "port-forward", "-n", $K8S_NAMESPACE, "svc/minio-service", "19000:9000" -PassThru -NoNewWindow
try {
    Start-Sleep -Seconds 3
    & $PYTHON_EXE -c @"
import os
from minio import Minio

client = Minio('127.0.0.1:19000', access_key='minioadmin', secret_key='minioadmin', secure=False)
for bucket in ['data', 'code', 'results', 'mapreduce-shuffle-overflow']:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

build_dir = r'$buildDir'
user_id = '$USER_ID'
for name in os.listdir(build_dir):
    if name.endswith('.class'):
        client.fput_object('code', f'{user_id}/{name}', os.path.join(build_dir, name))
        print(f'Uploaded code/{user_id}/{name}')
"@
} finally {
    if ($pf -and -not $pf.HasExited) {
        Stop-Process -Id $pf.Id -Force
    }
    Remove-Item -Recurse -Force $tmpRoot -ErrorAction SilentlyContinue
}

Write-Host "`n[SUCCESS] Kubernetes infrastructure is Ready and MinIO is seeded!" -ForegroundColor Green
