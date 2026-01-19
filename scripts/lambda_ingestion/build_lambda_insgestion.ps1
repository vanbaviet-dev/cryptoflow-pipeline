# =========================================
# build_lambda_ingestion.ps1
# =========================================

$ErrorActionPreference = "Stop"

# -------- Paths --------
$PROJECT_ROOT = Resolve-Path "..\.."
$LAMBDA_DIR   = "$PROJECT_ROOT\bronze\lambda_ingestion"
$APP_DIR      = "$LAMBDA_DIR\app"
$BUILD_DIR    = "$LAMBDA_DIR\build"
$ZIP_FILE     = "$BUILD_DIR\cryptoflow-ingestion.zip"
$REQ_FILE     = "$LAMBDA_DIR\requirements.txt"

Write-Host "== Build Lambda Ingestion =="

# -------- 1. Clean build directory --------
Write-Host "Cleaning build directory..."
if (Test-Path $BUILD_DIR) {
    Remove-Item "$BUILD_DIR\*" -Recurse -Force
} else {
    New-Item -ItemType Directory -Path $BUILD_DIR | Out-Null
}

# -------- 2. Copy app/ into build/ (exclude __pycache__) --------
Write-Host "Copying app source code (excluding __pycache__)..."

Copy-Item $APP_DIR $BUILD_DIR -Recurse -Force -Exclude "__pycache__"

# Safety: remove nested __pycache__ if any slipped through
Get-ChildItem -Path $BUILD_DIR -Recurse -Directory -Filter "__pycache__" |
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

# Ensure __init__.py exists
$initFile = "$BUILD_DIR\app\__init__.py"
if (!(Test-Path $initFile)) {
    New-Item -ItemType File -Path $initFile | Out-Null
}

# -------- 3. Install requirements into build/ --------
if (Test-Path $REQ_FILE) {
    $reqContent = Get-Content $REQ_FILE | Where-Object { $_ -and -not $_.StartsWith("#") }
    if ($reqContent.Count -gt 0) {
        Write-Host "Installing Python dependencies..."
        pip install -r $REQ_FILE -t $BUILD_DIR
    } else {
        Write-Host "requirements.txt is empty. Skipping dependency install."
    }
} else {
    Write-Host "No requirements.txt found. Skipping dependency install."
}

# -------- 4. Zip build/ contents --------
Write-Host "Creating deployment zip..."

if (Test-Path $ZIP_FILE) {
    Remove-Item $ZIP_FILE -Force
}

Push-Location $BUILD_DIR
Compress-Archive -Path * -DestinationPath $ZIP_FILE -Force
Pop-Location

Write-Host "Build completed successfully!"
Write-Host "Output: $ZIP_FILE"
