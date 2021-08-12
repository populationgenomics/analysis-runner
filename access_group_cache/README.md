# Access group cache

For inexplicable reasons, a Google Groups [`groups.lookup`](https://cloud.google.com/identity/docs/reference/rest/v1beta1/groups/lookup) and [`groups.memberships.lookup`](https://cloud.google.com/identity/docs/reference/rest/v1beta1/groups.memberships/lookup) REST call takes longer than 1.5s each from the `australia-southeast1` zone. In the US, it's a little faster, at 0.7s.

In order to avoid this surprisingly high latency, this Cloud Run deployment stores group membership lists in a separate secret per group. When invoked like a Cron job through Cloud Scheduler, those secrets can therefore act as a reasonably fast cache to facilitate membership checks.

To deploy, run:

```bash
gcloud beta run deploy access-group-cache \
  --source . \
  --platform=managed \
  --project=analysis-runner \
  --region=australia-southeast1 \
  --no-allow-unauthenticated \
  --service-account=access-group-cache@analysis-runner.iam.gserviceaccount.com
```

The `access-group-cache-refresh` Pub/Sub topic is configured to [trigger](https://cloud.google.com/run/docs/triggering/pubsub-push) the Cloud Run endpoint.
