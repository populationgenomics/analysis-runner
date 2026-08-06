from test.client.conftest import CliRunMocks

from analysis_runner.cli import main_from_args

ARGS = [
    '--dataset',
    'fewgenomes',
    '--access-level',
    'test',
    '--output-dir',
    'hello-world-test',
    '--description',
    'mock-test',
    'echo hello world',
]


def test_no_specified_mode_submits_analysisrunner(cli_run_mocks: CliRunMocks) -> None:
    main_from_args(ARGS)
    cli_run_mocks.analysis_runner.assert_called_once()
    cli_run_mocks.cromwell.assert_not_called()
    cli_run_mocks.config.assert_not_called()
    ns = cli_run_mocks.analysis_runner.call_args.args[0]
    assert ns.dataset == 'fewgenomes'
    assert ns.access_level == 'test'
    assert ns.output_dir == 'hello-world-test'


def test_analysis_runner_mode_submits_analysisrunner(
    cli_run_mocks: CliRunMocks,
) -> None:
    args = ['analysis-runner', *ARGS]
    main_from_args(args)
    cli_run_mocks.analysis_runner.assert_called_once()
    cli_run_mocks.cromwell.assert_not_called()
    cli_run_mocks.config.assert_not_called()
    ns = cli_run_mocks.analysis_runner.call_args.args[0]
    assert 'analysis-runner' not in ns


def test_cromwell_mode_submits_cromwell(cli_run_mocks: CliRunMocks) -> None:
    args = ['cromwell', 'submit', *ARGS]
    main_from_args(args)
    cli_run_mocks.analysis_runner.assert_not_called()
    cli_run_mocks.cromwell.assert_called_once()
    cli_run_mocks.config.assert_not_called()
    ns = cli_run_mocks.cromwell.call_args.args[0]
    assert ns.dataset == 'fewgenomes'
    assert ns.access_level == 'test'


def test_config_mode_submits_config(cli_run_mocks: CliRunMocks) -> None:
    args = ['config', *ARGS][:-3]
    main_from_args(args)
    cli_run_mocks.analysis_runner.assert_not_called()
    cli_run_mocks.analysis_runner.assert_not_called()
    cli_run_mocks.config.assert_called_once()
    ns = cli_run_mocks.config.call_args.args[0]
    assert ns.dataset == 'fewgenomes'
    assert ns.access_level == 'test'


def test_help(capsys, cli_run_mocks: CliRunMocks) -> None:  # noqa: ANN001
    args = ['-h']
    main_from_args(args)
    cli_run_mocks.analysis_runner.assert_not_called()
    cli_run_mocks.cromwell.assert_not_called()
    cli_run_mocks.config.assert_not_called()
    captured = capsys.readouterr()
    assert 'usage: analysis-runner [-h] [-v]' in captured.out
