#!/bin/bash

# Goal: 
# - Setup Google Cloud Storage environment for 
#   - HTTP-to-GCS data transfer.
#   - Spark job execution in a Dataproc cluster.
#
# Key Features:
# - Authenticates user to use GC CLI and provides ADC for API calls from the app
# - Automates the complete setup required for transferring files from
# public HTTP URLs to a GCS bucket using Storage Transfer Service (STS).
# - Project config, IAM roles, bucket creation, Master SA key, STS agent configuration.
#
# Prerequisites: 
# - gcloud CLI, src/config.ini, project IAM permissions.
# - Operator user must have : Service Account Administrator, Service account user
#
# Outputs: credentials.json, configured bucket, STS agent, ADC Application Credentials.
#
# Usage: 
# ./setup_data_services.sh

# __________________________ Debug Functions ______________________

print() {  
    # Print Log messages in green
    
    MESSAGE=$1  
    GREEN='\033[0;32m'
    NC='\033[0m' # No Color (Reset)
    echo -e "${GREEN}${MESSAGE}${NC}"    
}

run_cmd(){  
    # Run command, display it & exit on errors
    # Commands whose failure is not critical, shouldn't call this function.

    COMMAND_STRING="$*"
    # The command string is redirected to the terminal as STDERR, 
    # so that a variable can recover only the result of the command
    print "$COMMAND_STRING" >&2 
    eval "$COMMAND_STRING"

    RETURN_CODE=$?
    if [ $RETURN_CODE -ne 0 ]; then
        print "❌ Exit on error." >&2
        exit 1
    fi
}

# _________________________ Main Execution __________________________ 

print "--- Setup GC Storage environment for Transfer & Spark jobs ---"

# _____________________ Login to GC CLI ______________________

print "▶️ Step 1: Enable Shell script to run GC CLI commands"
run_cmd "gcloud auth login"  # output: access token for CLI

# Get GCS Account Info
PROJECT_ID=`run_cmd "gcloud projects list \
--filter='projectId:blent-sandbox-*' \
--format='value(projectId)' \
--limit=1"`
REGION=`grep "^REGION =" ./src/config.ini | cut -d" " -f 3`

# Set up gcloud CLI configuration
run_cmd "gcloud config set project '$PROJECT_ID'"
run_cmd "gcloud config set dataproc/region '$REGION'"
run_cmd "gcloud config set compute/region '$REGION'"
run_cmd "gcloud config list"


# ________________________ Create Bucket ____________________________

print "▶️ Step 2. Create Bucket"
BUCKET_NAME=`grep "^BUCKET_NAME =" ./src/config.ini | cut -d" " -f 3`
BUCKET_PATH="gs://$BUCKET_NAME"
REGION=`gcloud config get-value dataproc/region`
CMD="gcloud storage buckets create $BUCKET_PATH --location=$REGION"
eval "$CMD"  # Avoids error in case bucket exists 
run_cmd "gcloud storage buckets describe $BUCKET_PATH"


# _____________________ Login to GC API for app ______________________

print "▶️ Step 3. Enable Python app to call GC API methods"

# # Update "project" in local config_default
# run_cmd "gcloud config set project '$PROJECT_ID'"
# run_cmd "gcloud config list"

GCP_SCOPE=https://www.googleapis.com/auth/cloud-platform
run_cmd "gcloud auth application-default login --scopes=$GCP_SCOPE"
# output: ~/.config/gcloud/application_default_credentials.json for the app


# _________ Enable Operator to access Bucket & assign Roles _________

print "▶️ Step 4. Enable Operator to give R/W access to Bucket"
OPERATOR_USER_EMAIL=`gcloud config get-value account 2>/dev/null`
run_cmd "gcloud storage buckets add-iam-policy-binding $BUCKET_PATH \
    --member=user:$OPERATOR_USER_EMAIL \
    --role=roles/storage.objectAdmin"

# ____________________ Enable Master to manage Jobs _________________

print "▶️ Step 5. Enable Master to manage Jobs via credentials"
MASTER_ACCOUNT=`run_cmd "gcloud iam service-accounts list \
 --filter='displayName:Compute Engine default service account' \
 --format='value(email)'"`
# MASTER_ACCOUNT=`run_cmd 'gcloud iam service-accounts list \
#  --filter="displayName:Compute Engine default service account" \
#  --format="value(email)"'`

run_cmd "gcloud iam service-accounts keys create credentials.json \
 --iam-account=$MASTER_ACCOUNT"
# output: ./credentials.json for the Master

# __________________ Enable Workers to execute Jobs__________________

print "▶️ Step 6. Enable Workers to execute Jobs via STS Agent"

# 1. Get Info for Account Creation

# Get Project Number (used by Python app to call transfer methods)
PROJECT_NUMBER=`gcloud projects describe $PROJECT_ID --format='value(projectNumber)'`

# Get Email (the one based on the Project Number not on the Project Id)
STS_EMAIL="project-${PROJECT_NUMBER}@storage-transfer-service.iam.gserviceaccount.com"

# 2. Verify Operator is able to create STS Agent
REQUIRED_ROLE="bindings.role:roles/iam.serviceAccountAdmin"
run_cmd "gcloud projects get-iam-policy $PROJECT_ID \
    --flatten='bindings[].members' \
    --format='table(bindings.role)' \
    --filter='bindings.members:$OPERATOR_USER_EMAIL AND $REQUIRED_ROLE'"

# 3. Grant STS Agent an access to Bucket
# The account is implicitely created with the policy update
run_cmd "gcloud storage buckets add-iam-policy-binding gs://$BUCKET_NAME \
    --member=serviceAccount:$STS_EMAIL \
    --role=roles/storage.admin"

# _____________________ End of Main Process ______________________

print "✅ Success: STS Environment is ready to run app."
return 0

# _____________________ Annex: Transfer Workflow ______________________

Transfer Workflow: Actions & Required Permissions 

#|-----------------------------|----------|----------------------------|-------------|-------------|
#|         Action              |Component |      Required Role         |    Token    | GCP Backend |
#|-----------------------------|---------------------------------------|-------------|-------------|
| Create Bucket                | Operator | storage.admin              | Token       | GCS API     |
| Grant objectAdmin to Operator| Operator | storage.admin              | Token       | IAM Service |
| Create Key for Master SA     | Operator | iam.serviceAccountAdmin    | Token       | IAM API     |
| Create STS Agent             | Operator | serviceusage.service       | Token       | STS API     |
|                              |          | .UsageConsumer (incl.)     |             |             |
| Grant storage.admin to Worker| Operator | storage.admin              | Token       | IAM Service |
| Upload TSV File              | Operator | storage.objectAdmin        | Token       | GCS API     |
| Create/Run/Monitor Job       | Master   | storagetransfer.user (auto)| Credentials | STS API     |
| Perform Data Transfer        | Worker   | storage.admin              | Token       | GCS API     |
| List Transferred Files       | Operator | storage.objectViewer (auto)| ADC         | GCS API     |
#|-----------------------------|----------|----------------------------|-------------|-------------|

Operator: GCP User
Master: User-managed Service Account
Worker: Google-managed STS Agent

incl.: included in the role Project Owner of the Operator
auto : automatically granted