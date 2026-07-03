REGION="us-central1"
JOB_REL_PATH="src/etl_job.py"
BUCKET_PATH="gs://blent_spark_bucket4"
echo """
Make sure the following parameters in the script are correct. 
▶▶▶ REGION: $REGION
▶▶▶ JOB_REL_PATH: $JOB_REL_PATH
▶▶▶ BUCKET_PATH: $BUCKET_PATH
"""


echo "▶️ Step 1. Create a Bucket"
PROJECT_ID=`gcloud projects list \
    --filter="projectId:blent-sandbox-*" \
    --format="value(projectId)" \
    --limit=1`
echo "▶▶▶ PROJECT_ID: $PROJECT_ID"
gcloud config set project "$PROJECT_ID"
gcloud config set dataproc/region "$REGION"

echo "▶▶▶ gcloud config list"
gcloud config list

echo "▶▶▶ gcloud compute instances list"
 gcloud compute instances list

CLUSTER="main-cluster" 
echo "▶▶▶ CLUSTER: $CLUSTER"

echo "▶▶▶ gcloud storage buckets create $BUCKET_PATH --location=$REGION"
gcloud storage buckets create $BUCKET_PATH --location=$REGION
iRet=$?
sleep 2
if [ $iRet -ne 0 ]; then
    exit 1
fi

echo "▶️ Step 2. Upload the job script to the bucket"
JOB_PATH="${BUCKET_PATH}/${JOB_REL_PATH}"
echo "▶▶▶ JOB_PATH: $JOB_PATH"
echo "▶▶▶ gcloud storage cp ./src/etl_job.py $JOB_PATH"
gcloud storage cp ./src/etl_job.py $JOB_PATH


echo "▶️ Step 3. Upload the credentials to the bucket"
CREDENTIALS="${BUCKET_PATH}/config/credentials.json"
echo "▶▶▶ CREDENTIALS: $CREDENTIALS"

IAM_ACCOUNT=`gcloud iam service-accounts list \
 --filter="displayName:'Compute Engine default service account'" \
 --format="value(email)"`
echo "▶▶▶ IAM_ACCOUNT: $IAM_ACCOUNT"

# ToDO: Update comments
# 1. Generate the key file locally
# 2. Upload it to your main storage bucket so your cluster can see it
echo "▶▶▶ gcloud iam service-accounts keys create credentials.json \
 --iam-account=$IAM_ACCOUNT"
gcloud iam service-accounts keys create credentials.json \
 --iam-account=$IAM_ACCOUNT

echo "▶▶▶ gcloud storage cp credentials.json $CREDENTIALS"
gcloud storage cp credentials.json $CREDENTIALS

echo "▶▶▶ gcloud storage ls $BUCKET_PATH"
gcloud storage ls "$BUCKET_PATH"


echo "▶️ Step 4. Grant the STS account access to the bucket"
# 1. Fetch your active project number dynamically
# 2. Build the correct service account email string
PROJECT_NUMBER=`gcloud projects describe $PROJECT_ID \
 --format="value(projectNumber)"`
echo "▶▶▶ PROJECT_NUMBER: $PROJECT_NUMBER"

DOMAIN="storage-transfer-service.iam.gserviceaccount.com"
TRANSFER_SERVICE_ACCOUNT="project-${PROJECT_NUMBER}@${DOMAIN}"
echo "▶▶▶ TRANSFER_SERVICE_ACCOUNT: $TRANSFER_SERVICE_ACCOUNT"

echo "▶▶▶ gcloud storage buckets add-iam-policy-binding $BUCKET_PATH \
 --member=\"serviceAccount:$TRANSFER_SERVICE_ACCOUNT\" \
 --role=\"roles/storage.objectAdmin\""
gcloud storage buckets add-iam-policy-binding $BUCKET_PATH \
 --member="serviceAccount:$TRANSFER_SERVICE_ACCOUNT" \
 --role="roles/storage.objectAdmin"

echo "▶▶▶ gcloud storage buckets add-iam-policy-binding $BUCKET_PATH \
 --member=\"serviceAccount:$TRANSFER_SERVICE_ACCOUNT\" \
 --role=\"roles/storage.legacyBucketReader\""
gcloud storage buckets add-iam-policy-binding $BUCKET_PATH \
 --member="serviceAccount:$TRANSFER_SERVICE_ACCOUNT" \
 --role="roles/storage.legacyBucketReader"

echo "▶▶▶ gcloud storage buckets add-iam-policy-binding $BUCKET_PATH \
 --member=\"serviceAccount:$TRANSFER_SERVICE_ACCOUNT\" \
 --role=\"roles/storage.objectViewer\""
gcloud storage buckets add-iam-policy-binding $BUCKET_PATH \
 --member="serviceAccount:$TRANSFER_SERVICE_ACCOUNT" \
 --role="roles/storage.objectViewer"

# =========================================================
# Syntax
echo """
▶▶▶ Display variable values:
 ./prepare_job_submission.sh > ./log/prepare_job_submission.log
 grep -E "▶️ |▶" ./log/prepare_job_submission.log"
 
▶▶▶ Run the following command to submit the job to Dataproc:
 gcloud dataproc jobs submit pyspark $JOB_PATH --cluster=$CLUSTER --region=$REGION \
    --files=$CREDENTIALS

 """