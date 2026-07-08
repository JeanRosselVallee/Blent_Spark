#!/bin/bash

# Goal : transfer source raw-data files from AWS S3 to a GCS Bucket

MONTHS="2019-Oct 2019-Nov 2019-Dec 2020-Jan 2020-Feb 2020-Mar 2020-Apr"
SOURCE_FOLDER_URL="https://blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/9c15cb"
BUCKET="gs://blent_spark_bucket1"
TARGET_DIR="${BUCKET}/data/raw"

# File import to GCS sandbox TEMP_DIR (24GB) is faster than to $HOME (4.6GB)
TEMP_DIR="/tmp/data/raw"

# Function to timestamp log messages
LOG_PATH="./transfert.log"
rm -f "$LOG_PATH"
echo_t() {
    echo "[$(date +"%Y%m%d-%H:%M:%S")] $1" | tee -a "$LOG_PATH"
}

# Function to exit in case of error code returned by a command
exit_on_error() {
    exit_code=$1
    error_message=$2

    if [ "$exit_code" -ne 0 ]; then
        echo_t "❌ Error: $error_message failed. (Exit Code: $exit_code)"
        exit
    fi
}

# Parallel upload for best performance
gcloud config set storage/parallel_composite_upload_enabled True   
exit_on_error $? "parallel configuration"

# Bucket creation
gcloud storage ls $BUCKET > /dev/null 2>&1
if [ "$?" -ne 0 ]; then
    gcloud storage buckets create $BUCKET --location=europe-west3
fi
gcloud storage ls $BUCKET > /dev/null
exit_on_error $? "Bucket creation"

# Raw Folder creation in Sandbox
mkdir -p "$TEMP_DIR"

# Raw Folder creation in Bucket
gcloud storage cp /dev/null ${TARGET_DIR}/.temp > /dev/null 2>&1 
gcloud storage ls $TARGET_DIR > /dev/null
exit_on_error $? "Raw Folder creation"

# Transfer files from AWS via GCS Sandbox to a Bucket
for MONTH in $MONTHS
do
    FILE_NAME="${MONTH}.csv"
    TEMP_PATH="${TEMP_DIR}/$FILE_NAME"
    TARGET_PATH="${TARGET_DIR}/${FILE_NAME}"
    
    # Download source file to GCS Sandbox once
    gcloud storage ls "$TARGET_PATH" > /dev/null 2>&1 
    if [ "$?" -eq 0 ]; then
        echo_t "✅ File already transferred to Bucket: $FILE_NAME."
    else
        echo_t "Downloading $FILE_NAME from S3 to Google Dataproc disk..."
        FILE_URL="${SOURCE_FOLDER_URL}/${FILE_NAME}"
        wget -q --show-progress "$FILE_URL" -O "$TEMP_PATH"                   
        exit_on_error $? "download to sandbox"

        echo_t "Uploading $FILE_NAME from Dataproc disk to GCS Bucket..."
        gcloud storage cp "$TEMP_PATH" "$TARGET_PATH"        
        exit_on_error $? "upload to Bucket"
        
        echo_t "✅ Success: file $FILE_NAME transferred."
        rm -f "$TEMP_PATH"  # local copy deleted to preserve disk space
    fi
    
done
