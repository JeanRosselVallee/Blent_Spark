#!/bin/bash


# Goal: Prepare & orchestrate the data-pipeline workflow on Google Cloud
#
# Description:
#   Runs 4 scripts
#   - sets up the development environment
#   - configures GCS/Dataproc data services,
#   - transfers raw files from AWS S3 to a GCS bucket via Storage Transfer Service
#   - submits a Spark ETL job to Dataproc. 
#
# Key Features:
#   - Supports partial execution starting from a chosen phase.
#   - Provides the arguments to Transfer & Spark jobs
#   - For Spark job, automatic upload of dependencies/files to GCS
#   - Unified logging to console and timestamped log file
#
# Prerequisites:
#   - ./src/config.ini with LOG_DIR, BUCKET_NAME, REGION, etc.
#   - ./setup_env_dev.sh, ./setup_data_services.sh, 
#   - ./src/job_transfer.py, ./src/job_spark.py, ./src/lib_common.py
#
# Usage:
#   ./run_all_scripts.sh -f <START_PHASE> [-s <DATE_START>] [-e <DATE_END>] [-d <DESTINATION>]
#
# Outputs:
#   - Log file: ${LOG_DIR}/run_all_scripts_<YYYYMMDD_HHMM>.log
#   - GCS bucket populated with raw data and processed output (via Spark)
#   - Dataproc job named spark-etl-<YYYYMMDD_HHMM>
#
# Examples:
#   ./run_all_scripts.sh
#   ./run_all_scripts.sh -f transfer -s '2019-10-01 00:00:00' -e '2019-10-16 00:00:00'
#   ./run_all_scripts.sh -f spark -d 'gs://my-bucket/processed'

# _____________________________ Arguments ______________________________ 

# Help text
HELP_TEXT=$(cat << HELP_TEXT_END
Usage: $0 -f <START_PHASE> [-s <DATE_START>] [-e <DATE_END>] [-d '<DESTINATION>'

-f, --from_step   1|all (default), 2|login, 3|transfer, 4|spark|any
                    1/all      : skip nothing
                    2/login    : skip env setup
                    3/transfer : run transfer + Spark jobs
                    4/spark/*  : run only Spark job
-s, --date_start  'YYYY-MM-DD HH:MM:SS'  (optional, passed to both jobs)
-e, --date_end    'YYYY-MM-DD HH:MM:SS'  (optional, passed to both jobs)
-d, --destination 'gs://<BUCKET_NAME>/<SUBDIR>' (optional, passed to both jobs)
HELP_TEXT_END
)

# __________________________ Debug Functions ______________________

echo_color() {  
    # Print Log messages in green
    
    LOG_MESSAGE=$1  
    YELLOW_COLOR='\033[1;33m'
    NO_COLOR='\033[0m' # No Color (Reset)
    echo -e "${YELLOW_COLOR}${LOG_MESSAGE}${NO_COLOR}"
}
print_phase() {
    # Add a horizontal bar to phase subtitle

    BAR="========================="
    echo_color "${BAR} $1 ${BAR}"
}

run_command(){  
    # Run command, display it & exit on errors
    COMMAND_STRING="$*"
    echo_color "$COMMAND_STRING"
    eval "$COMMAND_STRING"

    COMMAND_RETURN_CODE=$?
    if [ $COMMAND_RETURN_CODE -ne 0 ]; then
        echo_color "❌ Exit on error." >&2
        exit 1
    fi
}

# _________________________ Log File Setup __________________________ 

LOG_DIR=`grep "^LOG_DIR =" ./src/config.ini | cut -d" " -f 3`
mkdir -p ${LOG_DIR}
SUFFIX=$(date +"%Y%m%d_%H%M")
LOG_FILE="${LOG_DIR}/run_all_scripts_${SUFFIX}.log"

# Redirect Shell output to both: console & log file
ESCAPE_CHAR=$'\x1b'
COLOR_CODE_REGEX="${ESCAPE_CHAR}\[[0-9;]*m"  # Font color code to omit in log file
exec > >(tee >(sed -r "s/$COLOR_CODE_REGEX//g" >> "$LOG_FILE")) 2>&1
# "exec > X" = Redirect 1 (stdout) to X
# ">(...)" = stdout is redirected to a command not to a file
# "sed -r '...'" = Removes the font color codes as regexp from stdout messages  
# "2>&1" = Redirect file descriptor 2 (std error) to 1 (stdout)


# _________________________ Parse Arguments __________________________ 

START_PHASE="1"  # Run all scripts by default
# Cf. config.ini for other default argument values

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
            JOB_ARGS="$JOB_ARGS --DESTINATION='${DESTINATION}/run_$SUFFIX'"
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

# Check argument "destination" is a subdir of Bucket
BUCKET_NAME=`grep "^BUCKET_NAME =" ./src/config.ini | cut -d" " -f 3`
BUCKET_FROM_PATH=$(echo "$DESTINATION" | cut -d'/' -f3)
if [ "X$BUCKET_FROM_PATH" != "X$BUCKET_NAME" ]; then
    echo_color "❌ Argument DESTINATION is not a subdir of Bucket."
    exit 1
fi


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
    print_phase "Phase 1) Setup development environment"
    # Dev packages (curl, venv, GC CLI) & app requirements, env variables, venv.
    chmod +x ./setup_env_dev.sh
    run_command "source ./setup_env_dev.sh"
fi

if [ $START_PHASE -le 2 ]; then
    print_phase "Phase 2) Setup data services"
    # GCS Bucket, GCP Dataproc Cluster, GCP Dataproc Job
    chmod +x ./setup_data_services.sh
    run_command "source ./setup_data_services.sh"
fi

if [ $START_PHASE -le 3 ]; then
    print_phase "Phase 3) Run Cloud-Transfer Job"
    # Transfer source raw files from AWS S3 to GCS Bucket
    run_command "python3 -m src.job_transfer $JOB_ARGS"
fi

print_phase "Phase 4) Run Spark Job"
# ETL-Process of raw files in GCS Bucket using Spark

GCS_PATH="gs://${BUCKET_NAME}"

# Upload required files to GCS Bucket:
FILEPATHS="src/config.ini src/job_spark.py src/lib_common.py"
for PATH_i in $FILEPATHS; do
    run_command "gcloud storage cp ./$PATH_i $GCS_PATH/$PATH_i"
done

if [ "X$JOB_ARGS" != "X" ]; then
    JOB_ARGS="-- $JOB_ARGS"
fi
run_command "gcloud dataproc jobs submit pyspark $GCS_PATH/src/job_spark.py \
--id=spark-etl-$SUFFIX \
--cluster=main-cluster --region=us-central1 \
--files='$GCS_PATH/src/config.ini' \
--py-files='$GCS_PATH/src/lib_common.py' $JOB_ARGS"

# _____________________ End of Main Process ______________________

print_phase "🏁 Success: Finished run_all_scripts.sh"
exit 0
