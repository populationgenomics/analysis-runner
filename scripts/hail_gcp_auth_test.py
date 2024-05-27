#!/usr/bin/env python3
from cpg_utils.config import get_driver_image
from cpg_utils.hail_batch import get_batch

b = get_batch('Hail GCP auth test')

j = b.new_bash_job('Three ls approaches')

j.image(get_driver_image())
j.command(
    """

set -x

unset GOOGLE_APPLICATION_CREDENTIALS
echo "GAC is: $GOOGLE_APPLICATION_CREDENTIALS"

gsutil ls -lh gs://cpg-fewgenomes-test/
gcloud storage ls gs://cpg-fewgenomes-test/

python3 -c '
from google.cloud import storage;
client = storage.Client();
bucket = client.get_bucket("cpg-fewgenomes-test");
print([a.name for a in bucket.list_blobs(prefix="", delimiter="/")])
'
    """,
)

b.run(wait=False)
