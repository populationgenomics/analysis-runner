"""Constants for analysis-runner"""

from typing import Optional

SERVER_ENDPOINT = 'https://server-a2pko7ameq-ts.a.run.app'
SERVER_TEST_ENDPOINT = 'https://server-test-a2pko7ameq-ts.a.run.app'
ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'
CROMWELL_URL = 'https://cromwell.populationgenomics.org.au'
CROMWELL_AUDIENCE = (
    '717631777761-ec4u8pffntsekut9kef58hts126v7usl.apps.googleusercontent.com'
)
GCLOUD_ACTIVATE_AUTH_BASE = [
    'gcloud',
    '-q',
    'auth',
    'activate-service-account',
    '--key-file=/gsa-key/key.json',
]
GCLOUD_ACTIVATE_AUTH = ' '.join(GCLOUD_ACTIVATE_AUTH_BASE)


def get_server_endpoint(
    server_url: Optional[str] = SERVER_ENDPOINT,
    is_test: Optional[bool] = False,
):
    """
    Get the server endpoint {production / test}
    Do it in a function so it's easy to fix if the logic changes
    """
    if is_test:
        return SERVER_TEST_ENDPOINT

    if not server_url:
        return SERVER_ENDPOINT

    return server_url
