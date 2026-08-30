# Sample metadata integration

This Cloud Function listens to the Pub/Sub topic of analysis-runner submissions and updates the corresponding sample-metadata project.

To deploy, run:

```bash
gcloud config set project analysis-runner

gcloud functions deploy sample_metadata \
     --runtime python311 \
     --region australia-southeast1 \
     --trigger-topic submissions \
     --service-account sample-metadata@analysis-runner.iam.gserviceaccount.com
```

To create a cloud function with a different Metamist url (like a development
metamist server) use the following command, where `SM_URL` is the custom Metamist url.

```bash
gcloud functions deploy metamist-dev \
     --gen2 \
     --runtime python311 \
     --region australia-southeast1 \
     --entry-point main \
     --source . \
     --trigger-topic submissions \
     --service-account sample-metadata@analysis-runner.iam.gserviceaccount.com \
     --set-env-vars "SM_URL=$SM_URL"
```

Note: Permissions
If you are deploying a new cloud function for the first time, the pub sub service
account (which is sample-metadata@analysis-runner.iam.gserviceaccount.com) needs
invoker permissions on the new cloud run service. You will need to run this once.
The service name will be the name of the function e.g. metamist-dev as is above.

```bash
# Add invoker to the sa for running your new function
gcloud run services add-iam-policy-binding metamist-dev \
  --member="serviceAccount:sample-metadata@analysis-runner.iam.gserviceaccount.com" \
  --role="roles/run.invoker" \
  --region=australia-southeast1 \
  --project=analysis-runner

# Add invoker permissions to the sa for calling the cloud run that handles
# the metamist api. e.g.
gcloud run services add-iam-policy-binding metamist-development \
  --member="serviceAccount:sample-metadata@analysis-runner.iam.gserviceaccount.com" \
  --role="roles/run.invoker" \
  --region=australia-southeast1 \
  --project=metamist-dev
```
