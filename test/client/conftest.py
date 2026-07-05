from typing import NoReturn

import pytest

import analysis_runner.util


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


def _perform_version_check() -> None:
    pass


analysis_runner.util._perform_version_check = _perform_version_check  # noqa:SLF001
