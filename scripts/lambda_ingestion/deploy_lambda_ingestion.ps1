# =========================================
# deploy_lambda_ingestion.ps1
# =========================================

$ErrorActionPreference = "Stop"

# -------- Config --------
$FUNCTION_NAME = "cryptoflow-ingestion"
$REGION        = "ap-southeast-1"

$PROJECT_ROOT  = Resolve-Path "..\.."
$LAMBDA_DIR    = "$PROJECT_ROOT\bronze\lambda_ingestion"
$ZIP_FILE      = "$LAMBDA_DIR\build\cryptoflow-ingestion.zip"

Write-Host "== Deploy Lambda Ingestion =="

# -------- 1. Check zip exists --------
if (!(Test-Path $ZIP_FILE)) {
    Write-Error "Zip file not found: $ZIP_FILE"
    Write-Error "Please run build_lambda_ingestion.ps1 first."
    exit 1
}

# -------- 2. Deploy to AWS Lambda --------
Write-Host "Updating Lambda function code..."
Write-Host "Function: $FUNCTION_NAME"
Write-Host "Region  : $REGION"

aws lambda update-function-code `
    --function-name $FUNCTION_NAME `
    --zip-file fileb://$ZIP_FILE `
    --region $REGION `
    --no-cli-pager

Write-Host "Deploy completed successfully!"
