# ruff: noqa: S105
import unittest
from typing import Any, Optional
from unittest.mock import MagicMock, patch

from analysis_runner._version import __version__
from analysis_runner.cli import main_from_args

IMPORT_AR_IDENTITY_TOKEN_PATH = (
    'analysis_runner.cli_analysisrunner.get_google_identity_token'
)
IMPORT_CR_IDENTITY_TOKEN_PATH = 'analysis_runner.cli_cromwell.get_google_identity_token'

REQUEST_POST_PATH = 'requests.post'
REQUEST_GET_PATH = 'requests.get'
VERSION_STR = f'analysis-runner v{__version__}'


class TestCliBasic(unittest.TestCase):
    @patch('builtins.print')
    def test_version_short(self, mock_print: MagicMock):
        main_from_args(['-v'])
        mock_print.assert_called_with(VERSION_STR)

    @patch('builtins.print')
    def test_version_full(self, mock_print: MagicMock):
        main_from_args(['--version'])
        mock_print.assert_called_with(VERSION_STR)

    @patch('builtins.print')
    def test_help_short(self, mock_print: MagicMock):
        main_from_args(['-h'])
        mock_print.assert_called()

    @patch('builtins.print')
    def test_help_full(self, mock_print: MagicMock):
        main_from_args(['--help'])
        mock_print.assert_called()


class MockResponse:
    def __init__(self, **kwargs: Any) -> None:
        self.raise_for_status = lambda: None

        for k, v in kwargs.items():
            setattr(self, k, v)


def apply_mock_behaviour(
    *,
    mock_post: Optional[MagicMock] = None,
    mock_identity_token: Optional[MagicMock] = None,
):
    if mock_post:
        mock_post.return_value = MockResponse(text='<mocked-url>')

    if mock_identity_token:
        mock_identity_token.return_value = '<mocked-identity-token>'


class TestCliAnalysisRunner(unittest.TestCase):
    ANALYSIS_RUNNER_ARGS = (
        '--dataset',
        'fewgenomes',
        '--access-level',
        'test',
        '--description',
        'mock-test',
        '--output-dir',
        'hello-world-test',
        'echo',
        'hello-world',
    )

    @patch(IMPORT_AR_IDENTITY_TOKEN_PATH)
    @patch(REQUEST_POST_PATH)
    def test_regular_cli(self, mock_post: MagicMock, mock_identity_token: MagicMock):
        apply_mock_behaviour(
            mock_post=mock_post,
            mock_identity_token=mock_identity_token,
        )

        main_from_args(self.ANALYSIS_RUNNER_ARGS)

        mock_post.assert_called()
        mock_identity_token.assert_called()

    @patch(IMPORT_AR_IDENTITY_TOKEN_PATH)
    @patch(REQUEST_POST_PATH)
    def test_cli_with_mode(self, mock_post: MagicMock, mock_identity_token: MagicMock):
        apply_mock_behaviour(
            mock_post=mock_post,
            mock_identity_token=mock_identity_token,
        )
        main_from_args(['analysis-runner', *self.ANALYSIS_RUNNER_ARGS])

        mock_post.assert_called()
        mock_identity_token.assert_called()


class TestCliCromwell(unittest.TestCase):
    @patch(IMPORT_CR_IDENTITY_TOKEN_PATH)
    @patch(REQUEST_POST_PATH)
    def test_submit_cli(self, mock_post: MagicMock, mock_identity_token: MagicMock):
        apply_mock_behaviour(
            mock_post=mock_post,
            mock_identity_token=mock_identity_token,
        )

        args = [
            'cromwell',
            'submit',
            '--dataset',
            'fewgenomes',
            '--access-level',
            'test',
            '--description',
            'mock-test',
            '--output-dir',
            'hello-world-test',
            'workflow.wdl',
        ]

        main_from_args(args)

        mock_post.assert_called()
        mock_identity_token.assert_called()


if __name__ == '__main__':
    unittest.main()
