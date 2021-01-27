# Server deployment using Cloud Run

This directory contains the server component of the analysis runner, which is
deployed as a Cloud Run container.

It uses `aiohttp` instead of `flask` because Hail Batch's async event queue
doesn't mix well with `flask`'s threads.

To deploy, run:

```bash
gcloud beta run deploy server --source . --region australia-southeast1 --no-allow-unauthenticated --platform managed
```

The (stable) service URL is https://server-a2pko7ameq-ts.a.run.app, which is
referenced in the [CLI tool](../cli).
