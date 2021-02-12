# Airtable integration

This Cloud Function listens to the Pub/Sub topic of analysis-runner submissions and updates a corresponding Airtable base.

To deploy, run:

```bash
gcloud config set project analysis-runner

gcloud functions deploy airtable \
     --runtime python37 \
     --region australia-southeast1 \
     --trigger-topic submissions \
     --service-account airtable@analysis-runner.iam.gserviceaccount.com
```
