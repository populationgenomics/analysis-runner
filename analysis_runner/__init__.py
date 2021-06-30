"""analysis_runner module initialization."""

import os
from typing import Optional


def output_path(filename: str, bucket_category: Optional[str] = None) -> str:
    """Returns a full GCS path for the given bucket category and filename.

    Examples
    --------

    Assuming that the analysis-runner has been invoked with
    `--dataset fewgenomes --access-level test --output 1kg_pca/v42`:

    >>> from analysis_runner import output_path
    >>> output_path('loadings.ht')
    'gs://cpg-fewgenomes-test/1kg_pca/v42/loadings.ht'
    >>> output_path('report.html', 'web')
    'gs://cpg-fewgenomes-test-web/1kg_pca/v42/report.html'

    Notes
    -----
    Requires the `DATASET`, `ACCESS_LEVEL`, and `OUTPUT` environment variables to be set.

    Parameters
    ----------
    filename : str
        A suffix to append to the bucket + output directory.
    bucket_category : str, optional
        A category like "upload", "tmp", "web". If omitted, defaults to the "main" and "test" buckets based on the access level. See
        https://github.com/populationgenomics/team-docs/tree/main/storage_policies
        for a full list of categories and their use cases.

    Returns
    -------
    str
    """
    dataset = os.getenv('DATASET')
    access_level = os.getenv('ACCESS_LEVEL')
    output = os.getenv('OUTPUT')
    assert dataset and access_level and output

    if bucket_category is None:
        bucket_category = 'test' if access_level == 'test' else 'main'
    elif bucket_category not in ('archive', 'upload'):
        bucket_category = f'{access_level}-{bucket_category}'

    return os.path.join('gs://', f'cpg-{dataset}-{bucket_category}', output, filename)
