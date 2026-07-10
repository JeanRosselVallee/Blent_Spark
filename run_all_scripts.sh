#!/bin/bash
set -e  # Stop if any command fails

# Goal
# Usage:
# Choose from which step to start the execution:
# - zero: Run all scripts from scratch
# - login: Skip setup of development environments
# - transfer: Run Transfer & Spark jobs
# - spark: Run only Spark job


# ToDo: add arg Bucket name but checkbefore if available in config_ini

# Help text
HELP_TEXT=$(cat << HELP_TEXT_END
Usage: $0 --from_step <STEP> [--s <DATE_START>] [--e <DATE_END>] [--d '<DESTINATION>'

-f, --from_step    1|all (default), 2|login, 3|transfer, 4|spark|any
-s, --date_start   'YYYY-MM-DD HH:MM:SS'         (Optional)
-e, --date_end     'YYYY-MM-DD HH:MM:SS'         (Optional)
-d, --destination  'gs://<BUCKET_NAME>/<SUBDIR>' (Optional)

N.B.: For argument "from_step", choose from which step to start the execution:
- "1" or "all":      Run all scripts from scratch
- "2" or "login":    Skip setup of development environments
- "3" or "transfer": Run Transfer & Spark jobs
- "4", "spark" or any other value: Run only Spark job
HELP_TEXT_END
)

# _________________________ Parse Arguments __________________________ 

STEP="1"  # Run all scripts by default
# Cf. config.ini for other default values

while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--from_step)
            STEP="$2"
            shift 2  # next argument
            ;;
        -s|--date_start)
            DATE_START="$2"
            shift 2
            ;;
        -e|--date_end)
            DATE_END="$2"
            shift 2
            ;;
        -d|--destination)
            DESTINATION="$2"
            shift 2
            ;;
        *)
            echo "$HELP_TEXT"
            exit 1
            exit 0
            ;;
    esac
done

# Display argument values
for VAR_NAME in STEP DATE_START DATE_END DESTINATION; do
    VAR_VALUE=$(eval "echo \$$VAR_NAME")
    if [ "X$VAR_VALUE" != "X" ]; then
        echo "$VAR_NAME: $VAR_VALUE"


# __________________________ Debug Functions ______________________

print() {  
    # Print Log messages in green
    
    MESSAGE=$1  
    YELLOW='\033[1;33m'
    NC='\033[0m' # No Color (Reset)
    echo -e "${YELLOW}${MESSAGE}${NC}"    r
}

run_cmd(){  
    # Run command, display it & exit on errors
    COMMAND_STRING="$*"
    print "$COMMAND_STRING"
    eval "$COMMAND_STRING"
}


# _________________________ Main Execution __________________________ 

case "$STEP" in
    1|all)
        # Step 1) Setup development environment:
        # Dev packages (curl, venv, GC CLI) & app requirements, env variables, venv.
        chmod +x ./setup_env_dev.sh
        run_cmd "source ./setup_env_dev.sh"
        ;;&  # execute next case
    2|login)
        # Step 2) Setup data services:
        # GCS Bucket, GCP Dataproc Cluster, GCP Dataproc Job
        chmod +x ./setup_data_services.sh
        run_cmd "source ./setup_data_services.sh"
        ;;&  # execute next case
    3|transfer)
        # Step 3) Run Cloud-Transfer Job: transfer source raw files from AWS S3 to GCS Bucket
        run_cmd "python3 -m src.job_transfer"
        ;;
esac

# Step 4) Run Spark Job: ETL-Process of raw files in GCS Bucket using Spark
GCS_PATH="gs://blent_spark_bucket5"

# Upload required files to GCS Bucket:
FILEPATHS="src/config.ini src/job_spark.py src/lib_common.py credentials.json"
for PATH_i in $FILEPATHS; do
    run_cmd "gcloud storage cp ./$PATH_i $GCS_PATH/$PATH_i"
done

run_cmd "gcloud dataproc jobs submit pyspark $GCS_PATH/src/job_spark.py \
--cluster=main-cluster --region=us-central1 \
--files='$GCS_PATH/credentials.json,$GCS_PATH/src/config.ini' \
--py-files='$GCS_PATH/src/lib_common.py' -- \
--DATE_START='2019-10-01 00:00:00' --DATE_END='2019-10-16 00:00:00'"

# _____________________ End of Main Process ______________________

print "🏁 Success: Finished run_all_scripts.sh"
exit 0