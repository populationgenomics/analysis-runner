"""
A Cloud Function to store analysis-runner submission metadata
in the Sample-Metadata database.
"""
import requests


def sample_metadata(data, unused_context):
    """Puts analysis in sample-metadata"""
    dataset = data.pop('dataset')
    output_dir = data.pop('output')
    data['source'] = 'analysis-runner'
    sm_data = {
        # we don't know the sample_ids unfortunately :(
        'sample_ids': [],
        'type': 'custom',
        'status': 'unknown',
        'output': output_dir,
        'author': data.pop('user'),
        'meta': data,
    }
    token = get_identity_token()
    requests.put(
        f'https://sample-metadata.populationgenomics.org.au/api/v1/analysis/{dataset}',
        sm_data,
        headers={'Authorization': f'Bearer {token}'},
    )


def get_identity_token() -> str:
    """
    Get identity token
    Source: https://cloud.google.com/functions/docs/securing/function-identity#identity_tokens
    """
    url = 'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity'
    r = requests.get(url=url, headers={'Metadata-Flavor': 'Google'})
    r.raise_for_status()
    return r.text
