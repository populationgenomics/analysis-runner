# Disk usage stats

[`disk_usage.py`](disk_usage.py) creates an aggregate disk usage report for all buckets of a dataset, by
explicitly listing all blobs. In contrast to `gsutil du`, we aggregate at a 2-level
folder depth or at any `.ht` or `.mt` level. Note that generating this report is somewhat expensive, as particularly for Hail (Matrix)Tables it can result in a large number of [Class B operations](https://cloud.google.com/storage/pricing#process-pricing).

Since listing blobs can take a long time, it's a good idea to use a non-preemptible machine to run this:

```sh
analysis-runner --dataset $DATASET --cpu 0.5 --no-preemptible --access-level standard --output-dir "disk_usage/$(date +'%Y-%m-%d')" --description "disk usage stats" disk_usage.py
```
