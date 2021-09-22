"""
A Cloud Function to store analysis-runner submission metadata
in the Sample-Metadata database.
"""
import json
import base64
import requests

AUDIENCE = 'https://sample-metadata-api-mnrpw3mdza-ts.a.run.app'


def sample_metadata(data, unused_context):
    """Puts analysis in sample-metadata"""

    metadata = json.loads(base64.b64decode(data['data']).decode('utf-8'))

    project = metadata.pop('dataset')
    output_dir = metadata.pop('output')
    metadata['source'] = 'analysis-runner'
    access_level = metadata.get('accessLevel')

    if access_level == 'test':
        project += '-test'

    sm_data = {
        'sample_ids': [],
        'type': 'custom',
        'status': 'unknown',
        'output': output_dir,
        'author': metadata.pop('user'),
        'meta': metadata,
        'active': False,
    }
    try:
        token = get_identity_token()
        r = requests.put(
            f'{AUDIENCE}/api/v1/analysis/{project}/',
            json=sm_data,
            headers={'Authorization': f'Bearer {token}'},
        )
        r.raise_for_status()
        analysis_id = r.text
        print(f'Created analysis with ID = {analysis_id}')
        return analysis_id
    except requests.exceptions.HTTPError as err:
        print(f'Failed with response: {err.response.text}')
        raise err


def get_identity_token() -> str:
    """
    Get identity token
    Source: https://cloud.google.com/functions/docs/securing/function-identity#identity_tokens
    """
    meta_url = 'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity'
    url = f'{meta_url}?audience={AUDIENCE}&format=full'
    r = requests.get(url=url, headers={'Metadata-Flavor': 'Google'})
    r.raise_for_status()
    return r.text
