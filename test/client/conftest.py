from typing import NamedTuple, NoReturn
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture


@pytest.fixture(autouse=True)
def _no_outbound_requests(monkeypatch) -> None:  # noqa: ANN001
    def _blocked(*args, **kwargs) -> NoReturn:  # noqa: ANN002, ANN003
        raise RuntimeError(
            f'Outbound requests are not allowed. Test attempted: args={args} kwargs={kwargs}',
        )

    monkeypatch.setattr('requests.post', _blocked)
    monkeypatch.setattr('requests.get', _blocked)


class _FakeAnalysisRunnerServerResponse:
    status_code = 200
    text = 'ok'

    def raise_for_status(self) -> None:
        pass


class CliRunMocks(NamedTuple):
    analysis_runner: MagicMock
    cromwell: MagicMock
    config: MagicMock


@pytest.fixture
def cli_run_mocks(mocker: MockerFixture) -> CliRunMocks:
    return CliRunMocks(
        analysis_runner=mocker.patch(
            'analysis_runner.cli.run_analysis_runner_from_args'
        ),
        cromwell=mocker.patch('analysis_runner.cli.run_cromwell_from_args'),
        config=mocker.patch('analysis_runner.cli.run_config_from_args'),
    )


@pytest.fixture
def posted_requests(monkeypatch) -> dict[str, str]:  # noqa: ANN001
    calls: dict[str, str] = {}

    def _post(url, **kwargs) -> _FakeAnalysisRunnerServerResponse:  # noqa: ANN001, ANN003
        calls.update({'url': url, **kwargs})
        return _FakeAnalysisRunnerServerResponse()

    monkeypatch.setattr('requests.post', _post)
    monkeypatch.setattr(
        'analysis_runner.cli_analysisrunner.get_google_identity_token',
        lambda endpoint: 'fake-token',  # noqa: ARG005
    )
    return calls


@pytest.fixture(autouse=True)
def _skip_version_check(monkeypatch) -> None:  # noqa: ANN001
    for module in (
        'analysis_runner.cli_analysisrunner',
        'analysis_runner.cli_cromwell',
        'analysis_runner.cli_config',
    ):
        monkeypatch.setattr(f'{module}._perform_version_check', lambda: None)
