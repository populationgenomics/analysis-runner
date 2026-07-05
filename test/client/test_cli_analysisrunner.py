import logging
from collections.abc import Iterator  # noqa: TC003
from typing import TypedDict

import pytest

from analysis_runner.cli_analysisrunner import confirm_choice, run_analysis_runner


class AnalysisRunnerArgs(TypedDict):
    dataset: str
    access_level: str
    description: str
    output_dir: str
    script: list[str]


ANALYSIS_RUNNER_ARGS: AnalysisRunnerArgs = {
    'dataset': 'fewgenomes',
    'access_level': 'test',
    'description': 'mock-test',
    'output_dir': 'hello-world-test',
    'script': [],
}


def test_run_analysis_runner_no_script_errors():
    with pytest.raises(ValueError, match=r'No script provided'):
        run_analysis_runner(**ANALYSIS_RUNNER_ARGS)


def test_run_analysis_runner_wrong_input_for_full_access(monkeypatch, capsys) -> None:  # noqa: ANN001
    responses: Iterator[str] = iter(['b', 'n'])
    monkeypatch.setattr(
        'builtins.input',
        lambda prompt: next(responses),  # noqa: ARG005
    )
    assert confirm_choice('Continue') is False
    captured = capsys.readouterr()
    assert captured.out == 'Unrecognised option, please try again.\n'


def test_run_analysis_runner_decline_full_access(monkeypatch) -> None:  # noqa: ANN001
    args: AnalysisRunnerArgs = {
        **ANALYSIS_RUNNER_ARGS,
        'access_level': 'full',
        'script': ['echo', 'hello world'],
    }
    monkeypatch.setattr(
        'builtins.input',
        lambda prompt: 'n',  # noqa: ARG005
    )
    with pytest.raises(SystemExit):
        run_analysis_runner(**args)


def test_run_analysis_runner_accept_full_access(
    monkeypatch,  # noqa: ANN001
    posted_requests,  # noqa: ANN001
    caplog,  # noqa: ANN001
) -> None:
    args: AnalysisRunnerArgs = {
        **ANALYSIS_RUNNER_ARGS,
        'access_level': 'full',
        'script': ['echo', 'hello world'],
    }
    caplog.set_level(logging.INFO, logger='analysis_runner')
    monkeypatch.setattr(
        'builtins.input',
        lambda prompt: 'y',  # noqa: ARG005
    )
    run_analysis_runner(**args)
    assert len(posted_requests) == 4  # noqa:PLR2004  # headers, json, timeout, url
    assert posted_requests['json']['accessLevel'] == 'full'
    assert posted_requests['headers']['Authorization'] == 'Bearer fake-token'
    assert 'Request submitted successfully: ok' in caplog.text
