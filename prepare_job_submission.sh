 BUCKET_PATH="gs://blent_spark_bucket2"
 REGION="us-central1"
 JOB_REL_PATH="src/etl_job.py"

 gcloud config list
 gcloud compute instances list

 gcloud storage ls "$BUCKET_PATH"
 gcloud storage buckets create $BUCKET_PATH --location=$REGION
 JOB_PATH="${BUCKET_PATH}/${JOB_REL_PATH}"
 gcloud storage cp ./src/etl_job.py $JOB_PATH
 gcloud dataproc jobs submit pyspark $JOB_PATH --cluster=main-cluster --region=$REGION