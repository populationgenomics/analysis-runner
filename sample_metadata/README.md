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
