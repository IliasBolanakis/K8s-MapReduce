<#
.SYNOPSIS
    Infrastructure Startup & MinIO Seeding Script
.DESCRIPTION
    Lifts Docker-compose backing services and provisions MinIO with
    the necessary buckets and files for the MapReduce framework.
#>

# =============================================================================
# 1. CONFIGURATION (VERIFY THESE PATHS)
# =============================================================================
$USER_ID             = "22222222-2222-2222-2222-222222222222"

# Project Roots
$NODE_ROOT           = "C:\Users\hlias\IdeaProjects\mapreduce-node"
$DOCKER_COMPOSE_PATH = Join-Path $NODE_ROOT "Docker-compose.yml"
$INIT_DB_SQL_PATH    = "C:\Users\hlias\IdeaProjects\mapreduce-manager\init-db.sql"

# Local Data Paths
$LOCAL_DATA_FILE     = "C:\Users\hlias\Desktop\Classes\Distributed\Code_And_Data\large_data.txt"
$LOCAL_MAPPER        = "C:\Users\hlias\Desktop\Classes\Distributed\Code_And_Data\WordCountMapper.class"
$LOCAL_REDUCER       = "C:\Users\hlias\Desktop\Classes\Distributed\Code_And_Data\WordCountReducer.class"

# =============================================================================
# 2. START INFRASTRUCTURE
# =============================================================================
Write-Host "`n[INFO] Starting Docker Compose Infrastructure..." -ForegroundColor Cyan

# Explicitly target the compose file in the node directory
docker-compose -f $DOCKER_COMPOSE_PATH down 2>$null
docker-compose -f $DOCKER_COMPOSE_PATH up -d

Write-Host "  -> Waiting for services to initialize (15 seconds)..." -ForegroundColor DarkGray
Start-Sleep -Seconds 15

# =============================================================================
# 3. INITIALIZE MINIO BUCKETS
# =============================================================================
Write-Host "`n[INFO] Configuring MinIO Buckets..." -ForegroundColor Cyan

# Set up an alias 'myminio' inside the container
docker exec local-minio mc alias set myminio http://localhost:9000 minioadmin minioadmin 2>$null

Write-Host "  -> Creating 'data', 'code', and 'results' buckets..." -ForegroundColor DarkGray
docker exec local-minio mc mb myminio/data --ignore-existing 2>$null
docker exec local-minio mc mb myminio/code --ignore-existing 2>$null
docker exec local-minio mc mb myminio/results --ignore-existing 2>$null

# =============================================================================
# 4. SEED MINIO WITH FILES
# =============================================================================
Write-Host "`n[INFO] Uploading artifacts to MinIO..." -ForegroundColor Cyan

if (Test-Path $LOCAL_DATA_FILE) {
    Write-Host "  -> Uploading dataset ($LOCAL_DATA_FILE)..." -ForegroundColor DarkGray
    docker cp $LOCAL_DATA_FILE "local-minio:/tmp/large_data.txt"
    docker exec local-minio mc cp /tmp/large_data.txt myminio/data/$USER_ID/large_data.txt
} else {
    Write-Host "  [WARNING] Local data file not found at $LOCAL_DATA_FILE" -ForegroundColor Yellow
}

if ((Test-Path $LOCAL_MAPPER) -and (Test-Path $LOCAL_REDUCER)) {
    Write-Host "  -> Uploading Java classes..." -ForegroundColor DarkGray
    docker cp $LOCAL_MAPPER "local-minio:/tmp/WordCountMapper.class"
    docker cp $LOCAL_REDUCER "local-minio:/tmp/WordCountReducer.class"

    docker exec local-minio mc cp /tmp/WordCountMapper.class myminio/code/$USER_ID/WordCountMapper.class
    docker exec local-minio mc cp /tmp/WordCountReducer.class myminio/code/$USER_ID/WordCountReducer.class
} else {
    Write-Host "  [WARNING] Java class files not found. Check your paths." -ForegroundColor Yellow
}

# =============================================================================
# 5. INITIALIZE POSTGRES
# =============================================================================
Write-Host "`n[INFO] Ensuring Postgres Database Schema is loaded..." -ForegroundColor Cyan
if (Test-Path $INIT_DB_SQL_PATH) {
    Get-Content $INIT_DB_SQL_PATH | docker exec -i local-postgres psql -U postgres -d dds_db | Out-Null
    Write-Host "  -> Database schema initialized." -ForegroundColor Green
} else {
    Write-Host "  [WARNING] Schema file not found at $INIT_DB_SQL_PATH" -ForegroundColor Yellow
}

Write-Host "`n[SUCCESS] Infrastructure is lifted and MinIO is seeded!" -ForegroundColor Green