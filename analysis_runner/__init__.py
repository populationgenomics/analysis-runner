"""analysis_runner module initialization."""

import os
from typing import Optional


def bucket_path(path: str, bucket_category: Optional[str] = None) -> str:
    """Returns a full GCS path for the given bucket category and path.

    This is useful for specifying input files, as in contrast to the output_path
    function, bucket_path does _not_ take the "output" parameter from the
    analysis-runner invocation into account.

    Examples
    --------
    Assuming that the analysis-runner has been invoked with
    `--dataset fewgenomes --access-level test --output 1kg_pca/v42`:

    >>> from analysis_runner import bucket_path
    >>> bucket_path('1kg_densified/combined.mt')
    'gs://cpg-fewgenomes-test/1kg_densified/combined.mt'
    >>> bucket_path('1kg_densified/report.html', 'web')
    'gs://cpg-fewgenomes-test-web/1kg_densified/report.html'

    Notes
    -----
    Requires the `DATASET` and `ACCESS_LEVEL` environment variables to be set.

    Parameters
    ----------
    path : str
        A path to append to the bucket.
    bucket_category : str, optional
        A category like "upload", "tmp", "web". If omitted, defaults to the "main" and
        "test" buckets based on the access level. See
        https://github.com/populationgenomics/team-docs/tree/main/storage_policies
        for a full list of categories and their use cases.

    Returns
    -------
    str
    """
    dataset = os.getenv('DATASET')
    access_level = os.getenv('ACCESS_LEVEL')
    assert dataset and access_level

    namespace = 'test' if access_level == 'test' else 'main'
    if bucket_category is None:
        bucket_category = namespace
    elif bucket_category not in ('archive', 'upload'):
        bucket_category = f'{namespace}-{bucket_category}'

    return os.path.join('gs://', f'cpg-{dataset}-{bucket_category}', path)


def output_path(path_suffix: str, bucket_category: Optional[str] = None) -> str:
    """Returns a full GCS path for the given bucket category and path suffix.

    In contrast to the bucket_path function, output_path takes the "output" parameter
    from the analysis-runner invocation into account.

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
    path_suffix : str
        A suffix to append to the bucket + output directory.
    bucket_category : str, optional
        A category like "upload", "tmp", "web". If omitted, defaults to the "main" and
        "test" buckets based on the access level. See
        https://github.com/populationgenomics/team-docs/tree/main/storage_policies
        for a full list of categories and their use cases.

    Returns
    -------
    str
    """
    output = os.getenv('OUTPUT')
    assert output
    return bucket_path(os.path.join(output, path_suffix), bucket_category)
