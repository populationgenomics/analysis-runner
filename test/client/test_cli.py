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


def test_no_specified_mode_submits_analysisrunner(mocker) -> None:  # noqa: ANN001
    mock_run_analysis_runner_from_args = mocker.patch(
        'analysis_runner.cli.run_analysis_runner_from_args',
    )
    mock_run_cromwell_from_args = mocker.patch(
        'analysis_runner.cli.run_cromwell_from_args',
    )
    mock_run_config_from_args = mocker.patch(
        'analysis_runner.cli.run_config_from_args',
    )
    main_from_args(ARGS)
    mock_run_analysis_runner_from_args.assert_called_once()
    mock_run_cromwell_from_args.assert_not_called()
    mock_run_config_from_args.assert_not_called()
    ns = mock_run_analysis_runner_from_args.call_args.args[0]
    assert ns.dataset == 'fewgenomes'
    assert ns.access_level == 'test'
    assert ns.output_dir == 'hello-world-test'


def test_analysis_runner_mode_submits_analysisrunner(mocker) -> None:  # noqa: ANN001
    args = ['analysis-runner', *ARGS]
    mock_run_analysis_runner_from_args = mocker.patch(
        'analysis_runner.cli.run_analysis_runner_from_args',
    )
    mock_run_cromwell_from_args = mocker.patch(
        'analysis_runner.cli.run_cromwell_from_args',
    )
    mock_run_config_from_args = mocker.patch(
        'analysis_runner.cli.run_config_from_args',
    )
    main_from_args(args)
    mock_run_analysis_runner_from_args.assert_called_once()
    mock_run_cromwell_from_args.assert_not_called()
    mock_run_config_from_args.assert_not_called()
    ns = mock_run_analysis_runner_from_args.call_args.args[0]
    assert 'analysis-runner' not in ns


def test_cromwell_mode_submits_cromwell(mocker) -> None:  # noqa: ANN001
    args = ['cromwell', 'submit', *ARGS]
    mock_run_analysis_runner_from_args = mocker.patch(
        'analysis_runner.cli.run_analysis_runner_from_args',
    )
    mock_run_cromwell_from_args = mocker.patch(
        'analysis_runner.cli.run_cromwell_from_args',
    )
    mock_run_config_from_args = mocker.patch(
        'analysis_runner.cli.run_config_from_args',
    )
    main_from_args(args)
    mock_run_analysis_runner_from_args.assert_not_called()
    mock_run_cromwell_from_args.assert_called_once()
    mock_run_config_from_args.assert_not_called()
    ns = mock_run_cromwell_from_args.call_args.args[0]
    assert ns.dataset == 'fewgenomes'
    assert ns.access_level == 'test'


def test_config_mode_submits_config(mocker) -> None:  # noqa: ANN001
    args = ['config', *ARGS][:-3]
    mock_run_analysis_runner_from_args = mocker.patch(
        'analysis_runner.cli.run_analysis_runner_from_args',
    )
    mock_run_cromwell_from_args = mocker.patch(
        'analysis_runner.cli.run_cromwell_from_args',
    )
    mock_run_config_from_args = mocker.patch(
        'analysis_runner.cli.run_config_from_args',
    )
    main_from_args(args)
    mock_run_analysis_runner_from_args.assert_not_called()
    mock_run_cromwell_from_args.assert_not_called()
    mock_run_config_from_args.assert_called_once()
    ns = mock_run_config_from_args.call_args.args[0]
    assert ns.dataset == 'fewgenomes'
    assert ns.access_level == 'test'


def test_help(capsys, mocker) -> None:  # noqa: ANN001
    args = ['-h']
    mock_run_analysis_runner_from_args = mocker.patch(
        'analysis_runner.cli.run_analysis_runner_from_args',
    )
    mock_run_cromwell_from_args = mocker.patch(
        'analysis_runner.cli.run_cromwell_from_args',
    )
    mock_run_config_from_args = mocker.patch(
        'analysis_runner.cli.run_config_from_args',
    )
    main_from_args(args)
    mock_run_analysis_runner_from_args.assert_not_called()
    mock_run_cromwell_from_args.assert_not_called()
    mock_run_config_from_args.assert_not_called()
    captured = capsys.readouterr()
    assert 'usage: analysis-runner [-h] [-v]' in captured.out
