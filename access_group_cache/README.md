# Access group cache

For inexplicable reasons, a Google Groups [`groups.lookup`](https://cloud.google.com/identity/docs/reference/rest/v1beta1/groups/lookup) and [`groups.memberships.lookup`](https://cloud.google.com/identity/docs/reference/rest/v1beta1/groups.memberships/lookup) REST call takes longer than 1.5s each from the `australia-southeast1` zone. In the US, it's a little faster, at 0.7s.

In order to avoid this surprisingly high latency, this Cloud Function stores all group memberships in a secret. When invoked like a Cron job through Cloud Scheduler, this secret can therefore act as a reasonably fast cache to check memberships.

To deploy, run:

```bash
gcloud functions deploy access_group_cache \
  --project=analysis-runner \
  --region=australia-southeast1 \
  --runtime=python39 \
  --trigger-topic=access-group-cache-refresh \
  --service-account=access-group-cache@analysis-runner.iam.gserviceaccount.com
```
