#!/bin/bash

# Databricks Asset Bundle Workflow Runner
# Usage: ./run_workflow.sh [--profile PROFILE] [--target TARGET] [OPTIONS]

set -e  # Exit on any error

# Default values
PROFILE=""
TARGET="dev"
SKIP_VALIDATION=false
SKIP_DEPLOYMENT=false
JOB_ID=""
UPLOAD_PDFS=false
DOWNLOAD_JSONL=false
PDFS_DIR="./pdfs"
JSONL_DIR="./jsonl"
VAR_ARGS=()

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to upload PDFs to Databricks volume
upload_pdfs() {
    local pdfs_dir="$1"
    local volume_path="$2"
    
    print_info "Uploading PDF files to Databricks volume..."
    print_info "Source: $pdfs_dir"
    print_info "Destination: $volume_path"
    
    # Check if PDFs directory exists
    if [[ ! -d "$pdfs_dir" ]]; then
        print_error "PDFs directory not found: $pdfs_dir"
        print_info "Create the directory and add PDF files, or use --pdfs-dir to specify a different location"
        return 1
    fi
    
    # Check if there are PDF files
    local pdf_count=$(find "$pdfs_dir" -name "*.pdf" -type f | wc -l)
    if [[ $pdf_count -eq 0 ]]; then
        print_warning "No PDF files found in: $pdfs_dir"
        print_info "Skipping PDF upload"
        return 0
    fi
    
    print_info "Found $pdf_count PDF file(s) to upload"
    
    # Upload each PDF file
    local uploaded=0
    local failed=0
    
    while IFS= read -r pdf_file; do
        local filename=$(basename "$pdf_file")
        print_info "Uploading: $filename"
        
        if databricks fs cp "$pdf_file" "$volume_path$filename" $PROFILE_ARG; then
            print_success "✓ $filename uploaded successfully"
            ((uploaded++))
        else
            print_error "✗ Failed to upload $filename"
            ((failed++))
        fi
    done < <(find "$pdfs_dir" -name "*.pdf" -type f)
    
    print_success "PDF upload completed: $uploaded uploaded, $failed failed"
    return 0
}

# Function to download JSONL files from Databricks volume
download_jsonl() {
    local volume_path="$1"
    local local_dir="$2"
    
    print_info "Downloading JSONL files from Databricks volume..."
    print_info "Source: $volume_path"
    print_info "Destination: $local_dir"
    
    # Create local directory if it doesn't exist
    if [[ ! -d "$local_dir" ]]; then
        print_info "Creating local directory: $local_dir"
        mkdir -p "$local_dir"
    fi
    
    # List files in the volume path
    print_info "Checking for JSONL files in volume..."
    local jsonl_files
    if ! jsonl_files=$(databricks fs ls "$volume_path" $PROFILE_ARG 2>/dev/null); then
        print_warning "Could not list files in volume path: $volume_path"
        print_info "This might be normal if no files were generated"
        return 0
    fi
    
    # Filter for JSONL files and text files (in case of part files)
    local file_count=0
    local downloaded=0
    local failed=0
    
    while IFS= read -r line; do
        # Skip directories and empty lines
        if [[ "$line" =~ ^d ]] || [[ -z "$line" ]]; then
            continue
        fi
        
        # Extract filename (last field)
        local filename=$(echo "$line" | awk '{print $NF}')
        
        # Download JSONL files and part files (including .txt files from text format)
        if [[ "$filename" == *.txt ]] || [[ "$filename" == part-* ]]; then
            print_info "Downloading: $filename"
            ((file_count++))
            
            # Determine local filename - rename .txt files to .jsonl
            local local_filename="$filename"
            if [[ "$filename" == *.txt ]]; then
                local_filename="${filename%.txt}.jsonl"
                print_info "  → Renaming to: $local_filename"
            fi
            
            if databricks fs cp "$volume_path$filename" "$local_dir/$local_filename" $PROFILE_ARG; then
                print_success "✓ $filename downloaded as $local_filename"
                ((downloaded++))
            else
                print_error "✗ Failed to download $filename"
                ((failed++))
            fi
        fi
    done <<< "$jsonl_files"
    
    if [[ $file_count -eq 0 ]]; then
        print_warning "No JSONL files found in volume path"
    else
        print_success "JSONL download completed: $downloaded downloaded, $failed failed"
    fi
    
    return 0
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

OPTIONS:
    --profile PROFILE       Databricks profile to use for authentication
    --target TARGET         Deployment target (dev or prod, default: dev)
    --skip-validation       Skip bundle validation step
    --skip-deployment       Skip bundle deployment step
    --job-id JOB_ID        Job ID to run (skip deployment and use existing job)
    --upload-pdfs           Upload PDF files from local directory to Databricks volume
    --pdfs-dir DIR          Local directory containing PDF files (default: ./pdfs)
    --download-jsonl        Download JSONL files from Databricks volume to local directory
    --jsonl-dir DIR         Local directory to save JSONL files (default: ./jsonl)
    --var KEY=VALUE        Override DAB variable (can be used multiple times)
    --help                  Show this help message

EXAMPLES:
    $0                                          # Use default profile and dev target
    $0 --profile my-profile --target prod       # Use specific profile and prod target
    $0 --job-id 123456 --profile my-profile     # Run specific job ID directly
    $0 --skip-validation --profile my-profile   # Skip validation step
    $0 --upload-pdfs --download-jsonl          # Full workflow with file management
    $0 --upload-pdfs --pdfs-dir ./documents --download-jsonl --jsonl-dir ./results
    $0 --var agent_choice=agent_bricks          # Override DAB variable
    $0 --var agent_choice=ai_query --var partition_count=10  # Multiple variables

WORKFLOW:
    1. Bundle validation (optional)
    2. Bundle deployment (optional)
    3. PDF upload to volume (if --upload-pdfs specified)
    4. Job execution
    5. JSONL download from volume (if --download-jsonl specified)

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --target)
            TARGET="$2"
            shift 2
            ;;
        --skip-validation)
            SKIP_VALIDATION=true
            shift
            ;;
        --skip-deployment)
            SKIP_DEPLOYMENT=true
            shift
            ;;
        --job-id)
            JOB_ID="$2"
            shift 2
            ;;
        --upload-pdfs)
            UPLOAD_PDFS=true
            shift
            ;;
        --pdfs-dir)
            PDFS_DIR="$2"
            shift 2
            ;;
        --download-jsonl)
            DOWNLOAD_JSONL=true
            shift
            ;;
        --jsonl-dir)
            JSONL_DIR="$2"
            shift 2
            ;;
        --var)
            VAR_ARGS+=("--var" "$2")
            shift 2
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate target
if [[ "$TARGET" != "dev" && "$TARGET" != "prod" ]]; then
    print_error "Invalid target: $TARGET. Must be 'dev' or 'prod'"
    exit 1
fi

# Build profile argument
PROFILE_ARG=""
if [[ -n "$PROFILE" ]]; then
    PROFILE_ARG="--profile $PROFILE"
fi

# Define volume paths (these match the defaults in databricks.yml)
SOURCE_VOLUME_PATH="dbfs:/Volumes/fins_genai/unstructured_documents/ai_parse_document_workflow/inputs/"
OUTPUT_VOLUME_PATH="dbfs:/Volumes/fins_genai/unstructured_documents/ai_parse_document_workflow/outputs/extracted_entities/"

print_info "Starting Databricks Asset Bundle Workflow"
print_info "Profile: ${PROFILE:-default}"
print_info "Target: $TARGET"

# Show file management options if enabled
if [[ "$UPLOAD_PDFS" == true ]]; then
    print_info "PDF Upload: $PDFS_DIR → $SOURCE_VOLUME_PATH"
fi
if [[ "$DOWNLOAD_JSONL" == true ]]; then
    print_info "JSONL Download: $OUTPUT_VOLUME_PATH → $JSONL_DIR"
fi

# Show variable overrides if any
if [[ ${#VAR_ARGS[@]} -gt 0 ]]; then
    print_info "Variable overrides:"
    for ((i=1; i<${#VAR_ARGS[@]}; i+=2)); do
        print_info "  - ${VAR_ARGS[$i]}"
    done
fi

# Step 1: Validate bundle (unless skipped or using existing job)
if [[ "$SKIP_VALIDATION" == false && -z "$JOB_ID" ]]; then
    print_info "Validating Databricks asset bundle..."
    if databricks bundle validate $PROFILE_ARG "${VAR_ARGS[@]}"; then
        print_success "Bundle validation completed successfully!"
    else
        print_error "Bundle validation failed!"
        exit 1
    fi
else
    print_warning "Skipping bundle validation"
fi

# Step 2: Deploy bundle (unless skipped or using existing job)
if [[ "$SKIP_DEPLOYMENT" == false && -z "$JOB_ID" ]]; then
    print_info "Deploying Databricks asset bundle to '$TARGET' target..."
    if databricks bundle deploy --target $TARGET $PROFILE_ARG "${VAR_ARGS[@]}"; then
        print_success "Bundle deployed successfully to '$TARGET' target!"
    else
        print_error "Bundle deployment failed!"
        exit 1
    fi
else
    print_warning "Skipping bundle deployment"
fi

# Step 3: Upload PDFs (if requested)
if [[ "$UPLOAD_PDFS" == true ]]; then
    if ! upload_pdfs "$PDFS_DIR" "$SOURCE_VOLUME_PATH"; then
        print_error "PDF upload failed!"
        exit 1
    fi
else
    print_info "Skipping PDF upload (use --upload-pdfs to enable)"
fi

# Step 4: Run the workflow
print_info "Running the workflow..."

if [[ -n "$JOB_ID" ]]; then
    # Use provided job ID
    print_info "Using provided job ID: $JOB_ID"
    if databricks jobs run-now --job-id $JOB_ID $PROFILE_ARG; then
        print_success "Workflow launched successfully!"
    else
        print_error "Failed to launch workflow!"
        exit 1
    fi
else
    # Use bundle run command
    print_info "Launching workflow via bundle..."
    if databricks bundle run ai_parse_document_workflow --target $TARGET $PROFILE_ARG "${VAR_ARGS[@]}"; then
        print_success "Workflow completed successfully!"
    else
        print_error "Workflow execution failed!"
        exit 1
    fi
fi

# Step 5: Download JSONL files (if requested)
if [[ "$DOWNLOAD_JSONL" == true ]]; then
    if ! download_jsonl "$OUTPUT_VOLUME_PATH" "$JSONL_DIR"; then
        print_warning "JSONL download encountered issues, but workflow completed successfully"
    fi
else
    print_info "Skipping JSONL download (use --download-jsonl to enable)"
fi

print_success "🎉 All done!"