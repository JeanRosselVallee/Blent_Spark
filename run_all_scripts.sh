#!/bin/bash

# Goal
# Usage:  ./run_all_scripts.sh
# Choose from which step to start the execution:
# - zero: Run all scripts from scratch
# - login: Skip setup of development environments
# - transfer: Run Transfer & Spark jobs
# - spark: Run only Spark job


# ToDo: add arg Bucket name but checkbefore if available in config_ini

# Help text
HELP_TEXT=$(cat << HELP_TEXT_END
Usage: $0 -f <START_PHASE> [-s <DATE_START>] [-e <DATE_END>] [-d '<DESTINATION>'

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

START_PHASE="1"  # Run all scripts by default
# Cf. config.ini for other default values

JOB_ARGS=""

# Check that a value is provided for each argument
if  [ `expr $# % 2` -ne 0 ]; then
    echo "Missing value in the arguments."
    echo "${HELP_TEXT}"
    exit 1
fi

while [ $# -gt 0 ]; do
    case $1 in
        -f|--from_step)
            START_PHASE="$2"
            shift 2  # next argument
            ;;
        -s|--date_start)
            DATE_START="$2"
            JOB_ARGS="$JOB_ARGS --DATE_START='$DATE_START'"
            shift 2
            ;;
        -e|--date_end)
            DATE_END="$2"
            JOB_ARGS="$JOB_ARGS --DATE_END='$DATE_END'"
            shift 2
            ;;
        -d|--destination)
            DESTINATION="$2"
            JOB_ARGS="$JOB_ARGS --DESTINATION='$DESTINATION'"
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
for VAR_NAME in START_PHASE DATE_START DATE_END DESTINATION; do
    VAR_VALUE=$(eval "echo \$$VAR_NAME")
    if [ "X$VAR_VALUE" != "X" ]; then
        echo "$VAR_NAME: $VAR_VALUE"
    fi
done

# __________________________ Debug Functions ______________________

print2() {  
    # Print Log messages in green
    
    MESSAGE=$1  
    YELLOW='\033[1;33m'
    NC='\033[0m' # No Color (Reset)
    echo -e "${YELLOW}${MESSAGE}${NC}"
}

run_cmd2(){  
    # Run command, display it & exit on errors
    COMMAND_STRING="$*"
    print2 "$COMMAND_STRING"
    eval "$COMMAND_STRING"

    RETURN_CODE=$?
    if [ $RETURN_CODE -ne 0 ]; then
        print2 "❌ Exit on error." >&2
        exit 1
    fi
}


# _________________________ Main Execution __________________________ 

# Map names to numbers
case "$START_PHASE" in
    2|login)    START_PHASE=2 ;;
    3|transfer) START_PHASE=3 ;;
    4|spark)    START_PHASE=4 ;;
    *)          START_PHASE=1 ;;
esac

# Activate Virtual Environment
source venv_spark/bin/activate

if [ $START_PHASE -eq 1 ]; then
    print2 "Phase 1) Setup development environment"
    # Dev packages (curl, venv, GC CLI) & app requirements, env variables, venv.
    chmod +x ./setup_env_dev.sh
    run_cmd2 "source ./setup_env_dev.sh"
fi

if [ $START_PHASE -le 2 ]; then
    print2 "Phase 2) Setup data services"
    # GCS Bucket, GCP Dataproc Cluster, GCP Dataproc Job
    chmod +x ./setup_data_services.sh
    run_cmd2 "source ./setup_data_services.sh"
fi

if [ $START_PHASE -le 3 ]; then
    print2 "Phase 3) Run Cloud-Transfer Job"
    # Transfer source raw files from AWS S3 to GCS Bucket
    run_cmd2 "python3 -m src.job_transfer $JOB_ARGS"
fi

print2 "Phase 4) Run Spark Job"
# ETL-Process of raw files in GCS Bucket using Spark
GCS_PATH="gs://blent_spark_bucket5"

# Upload required files to GCS Bucket:
FILEPATHS="src/config.ini src/job_spark.py src/lib_common.py credentials.json"
for PATH_i in $FILEPATHS; do
    run_cmd2 "gcloud storage cp ./$PATH_i $GCS_PATH/$PATH_i"
done

if [ "X$JOB_ARGS" != "X" ]; then
    JOB_ARGS="-- $JOB_ARGS"
fi
run_cmd2 "gcloud dataproc jobs submit pyspark $GCS_PATH/src/job_spark.py \
--cluster=main-cluster --region=us-central1 \
--files='$GCS_PATH/credentials.json,$GCS_PATH/src/config.ini' \
--py-files='$GCS_PATH/src/lib_common.py' $JOB_ARGS"

# _____________________ End of Main Process ______________________

print2 "🏁 Success: Finished run_all_scripts.sh"
exit 0