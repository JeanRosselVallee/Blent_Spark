#!/bin/bash

# Goal
# Usage:  ./run_all_scripts.sh
# Choose from which step to start the execution:
# - zero: Run all scripts from scratch
# - login: Skip setup of development environments
# - transfer: Run Transfer & Spark jobs
# - spark: Run only Spark job

# _____________________________ Usage ______________________________ 

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

# _________________________ Log File Setup __________________________ 

LOG_DIR=`grep "^LOG_DIR =" ./src/config.ini | cut -d" " -f 3`
mkdir -p ${LOG_DIR}
SUFFIX=$(date +"%Y%m%d_%H%M")
LOG_FILE="${LOG_DIR}/run_all_scripts_${SUFFIX}.log"

# Change Shell output to both: console & log file
ESCAPE_CHAR=$'\x1b'
COLOR_CODE_REGEX="${ESCAPE_CHAR}\[[0-9;]*m"  # Font color code to omit in log file
# exec > >(tee -a "$LOG_FILE" | sed -r "s/$COLOR_CODE_REGEX//g") 2>&1
exec > >(tee >(sed -r "s/$COLOR_CODE_REGEX//g" >> "$LOG_FILE")) 2>&1
# "exec > X" = Redirect 1 (stdout) to X
# ">(...)" = stdout is redirected to a command not to a file
# "sed -r '...'" = Removes the font color codes as regexp from stdout messages  
# "2>&1" = Redirect file descriptor 2 (std error) to 1 (stdout)


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
GCS_PATH="gs://blent_spark_bucket5"

# Upload required files to GCS Bucket:
FILEPATHS="src/config.ini src/job_spark.py src/lib_common.py"  # ToDo: credentials.json"
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
# --files='$GCS_PATH/credentials.json,$GCS_PATH/src/config.ini' \

# _____________________ End of Main Process ______________________

print_phase "🏁 Success: Finished run_all_scripts.sh"
exit 0
