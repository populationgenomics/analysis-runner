"""Constants for analysis-runner"""

SERVER_ENDPOINT = 'https://server-a2pko7ameq-ts.a.run.app'
SERVER_TEST_ENDPOINT = 'https://server-test-a2pko7ameq-ts.a.run.app'
ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'
CROMWELL_URL = 'https://cromwell.populationgenomics.org.au'
CROMWELL_AUDIENCE = (
    '717631777761-ec4u8pffntsekut9kef58hts126v7usl.apps.googleusercontent.com'
)
GCLOUD_ACTIVATE_AUTH = (
    'gcloud -q auth activate-service-account --key-file=/gsa-key/key.json'
)


def get_server_endpoint(is_test: bool = False):
    """
    Get the server endpoint {production / test}
    Do it in a function so it's easy to fix if the logic changes
    """
    if is_test:
        return SERVER_TEST_ENDPOINT

    return SERVER_ENDPOINT
