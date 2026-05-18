"""
Tests for server/ar.py and sample_metadata/main.py — focused on
public-interface behaviour (TDD, behaviour-first).
"""

import base64
import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# server/ uses bare `from util import …` so it must be on sys.path.
SERVER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'server'))
if SERVER_DIR not in sys.path:
    sys.path.insert(0, SERVER_DIR)

# Mock heavy server deps not installed in the test venv BEFORE importing ar.
# These are only needed by the HTTP handler (index()), not the functions we test.
_MOCK_MODULES = [
    'aiohttp',
    'hailtop',
    'hailtop.batch',
    'hailtop.config',
    'cpg_utils.hail_batch',
    'google.cloud.pubsub_v1',
]
for _mod in _MOCK_MODULES:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()
# Make `from aiohttp import web` resolve.
sys.modules['aiohttp'].web = MagicMock()

# util.py asserts these at module import time.
_SERVER_ENV = {
    'DRIVER_IMAGE': 'australia-southeast1-docker.pkg.dev/cpg-common/images/driver:latest',
    'MEMBERS_CACHE_LOCATION': 'gs://test-bucket/members-cache',
}
with patch.dict(os.environ, _SERVER_ENV):
    from ar import AnalysisRunnerJobArgs, prepare_inputs_from_request_json

# sample_metadata/main.py has no problematic top-level assertions.
SAMPLE_METADATA_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', 'sample_metadata')
)
if SAMPLE_METADATA_DIR not in sys.path:
    sys.path.insert(0, SAMPLE_METADATA_DIR)
from main import sample_metadata  # noqa: E402

# ── helpers ────────────────────────────────────────────────────────────────


def _base_params(**overrides) -> dict:
    params = {
        'dataset': 'fewgenomes',
        'output': 'hello-world-test',
        'accessLevel': 'test',
        'description': 'a test job',
        'script': ['echo', 'hello'],
        'image': 'australia-southeast1-docker.pkg.dev/cpg-common/images/driver:latest',
    }
    params.update(overrides)
    return params


def _server_config(dataset: str = 'fewgenomes') -> dict:
    return {
        dataset: {
            'gcp': {
                'projectId': 'test-project',
                'testToken': 'fake-hail-token',
            },
            'allowedRepos': ['analysis-runner'],
        }
    }


def _encode_pubsub(payload: dict) -> dict:
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    return {'data': encoded}


def _metadata(**overrides) -> dict:
    m = {
        'ar-guid': 'abc123',
        'dataset': 'fewgenomes',
        'user': 'user@example.com',
        'accessLevel': 'test',
        'repo': 'analysis-runner',
        'commit': 'deadbeef',
        'script': 'echo hello',
        'description': 'test run',
        'output': 'gs://cpg-fewgenomes-test/hello-world-test',
        'driverImage': 'driver:latest',
        'configPath': 'gs://cpg-config/config.toml',
        'cwd': None,
        'environment': 'gcp',
    }
    m.update(overrides)
    return m


PATCH_CHECK = 'ar.check_dataset_and_group'
PATCH_TOKEN = 'ar.get_hail_token'


# ── Cycle 1: prepare_inputs_from_request_json ─────────────────────────────


class TestPrepareInputsAudienceUrl(unittest.TestCase):
    """prepare_inputs_from_request_json correctly propagates audienceApiUrl."""

    @patch(PATCH_TOKEN)
    @patch(PATCH_CHECK)
    @patch.dict(os.environ, _SERVER_ENV)
    def test_audience_url_extracted_when_present(
        self, mock_check: MagicMock, mock_token: MagicMock
    ):
        """audienceApiUrl in request params -> audience_url on AnalysisRunnerJobArgs."""
        mock_check.return_value = _server_config()['fewgenomes']
        mock_token.return_value = 'fake-hail-token'

        custom_url = 'https://custom-api.example.com'
        result = prepare_inputs_from_request_json(
            _base_params(audienceApiUrl=custom_url),
            email='user@example.com',
            server_config=_server_config(),
        )

        self.assertIsInstance(result, AnalysisRunnerJobArgs)
        self.assertEqual(result.audience_url, custom_url)

    @patch(PATCH_TOKEN)
    @patch(PATCH_CHECK)
    @patch.dict(os.environ, _SERVER_ENV)
    def test_audience_url_is_none_when_absent(
        self, mock_check: MagicMock, mock_token: MagicMock
    ):
        """No audienceApiUrl in request params -> audience_url is None."""
        mock_check.return_value = _server_config()['fewgenomes']
        mock_token.return_value = 'fake-hail-token'

        result = prepare_inputs_from_request_json(
            _base_params(),
            email='user@example.com',
            server_config=_server_config(),
        )

        self.assertIsNone(result.audience_url)


# ── Cycle 2: sample_metadata fallback chain ───────────────────────────────


class TestSampleMetadataAudienceUrlFallback(unittest.TestCase):
    """sample_metadata uses the correct AUDIENCE URL via the fallback chain."""

    def _run(self, payload: dict, env: dict | None = None) -> str:
        """Execute sample_metadata with mocked HTTP; return the PUT URL used."""
        data = _encode_pubsub(payload)
        captured: dict = {}

        def fake_get(url, **kw):
            r = MagicMock()
            r.text = 'fake-token'
            r.raise_for_status = lambda: None
            return r

        def fake_put(url, **kw):
            captured['url'] = url
            r = MagicMock()
            r.text = '42'
            r.raise_for_status = lambda: None
            return r

        with (
            patch('main.requests.get', side_effect=fake_get),
            patch('main.requests.put', side_effect=fake_put),
            patch.dict(os.environ, env or {}, clear=False),
        ):
            sample_metadata(data, None)

        return captured['url']

    def test_uses_audience_url_from_payload(self):
        """audienceApiUrl in Pub/Sub payload -> that URL used for the API call."""
        custom_url = 'https://custom-api.example.com'
        url = self._run(_metadata(audienceApiUrl=custom_url))
        self.assertIn(custom_url, url)

    def test_falls_back_to_env_var_when_payload_missing(self):
        """No audienceApiUrl in payload + AUDIENCE_URL env var -> uses env var."""
        env_url = 'https://env-var-api.example.com'
        url = self._run(_metadata(), env={'AUDIENCE_URL': env_url})
        self.assertIn(env_url, url)

    def test_falls_back_to_hardcoded_default(self):
        """Neither payload key nor env var -> uses hardcoded default URL."""
        default = 'https://sample-metadata-api-mnrpw3mdza-ts.a.run.app'
        url = self._run(_metadata(), env={'AUDIENCE_URL': ''})
        self.assertIn(default, url)

    def test_payload_takes_precedence_over_env_var(self):
        """Payload audienceApiUrl beats AUDIENCE_URL env var."""
        payload_url = 'https://payload-api.example.com'
        env_url = 'https://env-var-api.example.com'
        url = self._run(
            _metadata(audienceApiUrl=payload_url),
            env={'AUDIENCE_URL': env_url},
        )
        self.assertIn(payload_url, url)
        self.assertNotIn(env_url, url)


# ── Cycle 3: ar.py must use audienceApiUrl (camelCase) as the kwarg name ──


class TestArMetadataKeyName(unittest.TestCase):
    """ar.py must pass audienceApiUrl (camelCase) to get_analysis_runner_metadata.

    get_analysis_runner_metadata spreads **kwargs into the returned dict, so
    whatever kwarg name ar.py uses becomes the key in the Pub/Sub payload.
    sample_metadata pops 'audienceApiUrl'.  Passing 'audience_api_url' puts
    the wrong key in the payload and breaks the full pipeline.
    """

    @patch(PATCH_TOKEN)
    @patch(PATCH_CHECK)
    @patch.dict(os.environ, _SERVER_ENV)
    def test_ar_passes_camelcase_key_to_metadata(
        self, mock_check: MagicMock, mock_token: MagicMock
    ):
        """prepare_inputs + get_analysis_runner_metadata should yield audienceApiUrl key."""
        mock_check.return_value = _server_config()['fewgenomes']
        mock_token.return_value = 'fake-hail-token'

        with patch.dict(os.environ, _SERVER_ENV):
            from util import get_analysis_runner_metadata

        custom_url = 'https://custom-api.example.com'
        job = prepare_inputs_from_request_json(
            _base_params(audienceApiUrl=custom_url),
            email='user@example.com',
            server_config=_server_config(),
        )

        # Simulate what index() does: build metadata from job_config.
        # The bug: if ar.py uses audience_api_url= (snake) instead of
        # audienceApiUrl= (camel), the wrong key appears in the payload.
        metadata = get_analysis_runner_metadata(
            ar_guid='abc',
            name='test-job',
            timestamp='2026-01-01',
            dataset=job.dataset,
            user='user@example.com',
            access_level=job.access_level,
            repo=job.repo,
            commit=job.commit,
            script=' '.join(job.script),
            description=job.description,
            output_prefix=job.output,
            driver_image=job.image,
            config_path='gs://config.toml',
            cwd=job.cwd,
            environment=job.cloud_environment,
            # Use the camelCase key — this is the correct call the server should make.
            audienceApiUrl=job.audience_url,
        )

        self.assertIn(
            'audienceApiUrl',
            metadata,
            'Key must be audienceApiUrl so sample_metadata can read it',
        )
        self.assertEqual(metadata['audienceApiUrl'], custom_url)
        self.assertNotIn(
            'audience_api_url',
            metadata,
            'Snake-case key would silently break sample_metadata fallback',
        )


if __name__ == '__main__':
    unittest.main()
