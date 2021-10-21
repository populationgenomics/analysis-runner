# flake8: noqa
# pylint: skip-file
from enum import Enum
from textwrap import indent
from typing import List, Dict, Optional
import datetime

from tabulate import tabulate

from analysis_runner.util import AnsiiColors


class ExecutionStatus(Enum):
    preparing = 'preparing'
    starting = 'starting'
    in_progress = 'inprogress'
    running = 'running'
    done = 'done'
    succeeded = 'succeeded'
    failed = 'failed'
    retryablefailure = 'retryablefailure'
    queuedincromwell = 'queuedincromwell'

    def __str__(self):
        return self.value

    @property
    def _symbols(self):
        return {
            ExecutionStatus.starting: '...',
            ExecutionStatus.preparing: '...',
            ExecutionStatus.queuedincromwell: '...',
            ExecutionStatus.in_progress: '~',
            ExecutionStatus.running: '~',
            ExecutionStatus.done: '#',
            ExecutionStatus.succeeded: '#',
            ExecutionStatus.failed: '!',
            ExecutionStatus.retryablefailure: '~!',
        }

    @property
    def _colors(self):
        return {
            ExecutionStatus.done: AnsiiColors.BRIGHTGREEN,
            ExecutionStatus.succeeded: AnsiiColors.BRIGHTGREEN,
            ExecutionStatus.failed: AnsiiColors.BRIGHTRED,
            ExecutionStatus.retryablefailure: AnsiiColors.BRIGHTBLUE,
        }

    def symbol(self):
        return self._symbols.get(self, '?')

    def color(self):
        return self._colors.get(self, AnsiiColors.RESET)

    def is_finished(self):
        _finished_states = {
            ExecutionStatus.done,
            ExecutionStatus.succeeded,
            ExecutionStatus.failed,
            ExecutionStatus.retryablefailure,
        }
        return self in _finished_states


class WorkflowMetadataModel:
    def __init__(
        self,
        workflowName=None,
        workflowProcessingEvents=None,
        metadataSource=None,
        actualWorkflowLanguageVersion=None,
        submittedFiles=None,
        calls: Dict[str, List['CallMetadata']] = None,
        outputs=None,
        workflowRoot=None,
        actualWorkflowLanguage=None,
        id=None,
        inputs=None,
        labels=None,
        submission=None,
        status=None,
        end=None,
        start=None,
        **kwargs,
    ):
        self.workflowName = workflowName
        self.workflowProcessingEvents = workflowProcessingEvents
        self.metadataSource = metadataSource
        self.actualWorkflowLanguageVersion = actualWorkflowLanguageVersion
        self.submittedFiles = submittedFiles
        self.calls = calls
        self.outputs = outputs
        self.workflowRoot = workflowRoot
        self.actualWorkflowLanguage = actualWorkflowLanguage
        self.id = id
        self.inputs = inputs
        self.labels = labels
        self.submission = submission
        self.status = (
            ExecutionStatus(status.lower()) if status else ExecutionStatus.preparing
        )
        self.end = end
        self.start = start

        # safety
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    @staticmethod
    def parse(d):
        new_d = {**d}
        calls_d = new_d.pop('calls')
        calls = {}
        for name, sublist in calls_d.items():
            name = name.split(".")[-1]
            calls[name] = sorted(
                [CallMetadata.parse({'name': name, **call}) for call in sublist],
                key=lambda c: f'{c.shardIndex or 0}-{c.start}',
            )

        return WorkflowMetadataModel(calls=calls, **new_d)

    def display(self, expand_completed=False, monochrome=False):

        duration_seconds = get_seconds_duration_between_cromwell_dates(
            self.start, self.end
        )
        duration_str = get_readable_duration(duration_seconds)

        headers = [
            ('Workflow ID', self.id),
            ('Name', self.workflowName),
            ('Status', self.status),
            ('Start', self.start),
            ('End', self.end),
            ('Duration', duration_str),
        ]

        calls_display: List[str] = []
        for name, calls in sorted(
            self.calls.items(), key=lambda a: a[1][0].start or '0'
        ):
            calls_display.append(
                indent(
                    prepare_inner_calls_string(
                        name,
                        calls,
                        expand_completed=expand_completed,
                        monochrome=monochrome,
                    ),
                    '  ',
                )
            )

        header = tabulate(headers)
        calls_str = '\n'.join(calls_display)
        return f"""
{header}
Jobs:
{calls_str}
"""


class CallMetadata:
    """Python model for cromwell CallMetadata"""

    def __init__(
        self,
        name,
        executionStatus,
        stdout=None,
        backendStatus=None,
        compressedDockerSize=None,
        commandLine=None,
        shardIndex=None,
        outputs=None,
        runtimeAttributes=None,
        callCaching=None,
        inputs=None,
        returnCode=None,
        jobId=None,
        backend=None,
        end=None,
        dockerImageUsed=None,
        stderr=None,
        callRoot=None,
        attempt=None,
        executionEvents=None,
        start=None,
        preemptible=None,
        jes=None,
        failures=None,
        calls: Optional[Dict[str, List['CallMetadata']]] = None,
        **kwargs,
    ):
        self.name = name
        self.executionStatus = (
            ExecutionStatus(executionStatus.lower()) if executionStatus else None
        )
        self.stdout = stdout
        self.backendStatus = backendStatus
        self.compressedDockerSize = compressedDockerSize
        self.commandLine = commandLine
        self.shardIndex = int(shardIndex) if shardIndex else None
        self.outputs = outputs
        self.runtimeAttributes = runtimeAttributes
        self.callCaching = callCaching
        self.inputs = inputs
        self.returnCode = returnCode
        self.jobId = jobId
        self.backend = backend
        self.end = end
        self.dockerImageUsed = dockerImageUsed
        self.stderr = stderr
        self.callRoot = callRoot
        self.attempt = attempt
        self.executionEvents = executionEvents
        self.start = start
        self.preemptible = preemptible
        self.calls = calls
        self.jes = jes
        self.failures = failures

        # safety
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    @staticmethod
    def parse(d):
        new_d = {**d}
        calls = None
        if 'subWorkflowMetadata' in new_d:
            calls_d = new_d.pop('subWorkflowMetadata').get('calls')
            calls = {}
            for name, sublist in calls_d.items():
                name = name.split(".")[-1]
                calls[name] = sorted(
                    [CallMetadata.parse({'name': name, **call}) for call in sublist],
                    key=lambda c: (c.shardIndex or 0, c.start),
                )
        return CallMetadata(calls=calls, **new_d)

    def display(self, expand_completed=False, monochrome=False):
        duration_str = get_readable_duration(
            get_seconds_duration_between_cromwell_dates(self.start, self.end)
        )

        extras = []
        is_done = self.executionStatus.is_finished()
        has_succeded = self.executionStatus == ExecutionStatus.succeeded
        if (not has_succeded or expand_completed) and self.calls:
            for name, calls in sorted(
                self.calls.items(), key=lambda a: a[1][0].start or '0'
            ):
                extras.append(
                    indent(
                        prepare_inner_calls_string(
                            name,
                            calls,
                            expand_completed=expand_completed,
                            monochrome=monochrome,
                        ),
                        '  ',
                    )
                )

        if not is_done:
            if self.jobId:
                extras.append(f'JobID: {self.jobId}')
        else:
            if self.callCaching and self.callCaching.get('hit'):
                extras.append(f'Call caching: true')

        if not self.calls and self.executionStatus == ExecutionStatus.failed:
            extras.append(f'stdout: {self.stdout}')
            extras.append(f'stderr: {self.stderr}')
            extras.append(f'rc: {self.returnCode}')

        if self.failures:
            caused_by = unwrap_caused_by(self.failures)
            if caused_by:
                extras.append(f'error: {caused_by}')

        name = self.name

        if self.shardIndex is not None and self.shardIndex >= 0:
            name += f' (shard-{self.shardIndex})'

        if self.attempt is not None and self.attempt > 1:
            name += f' (attempt {self.attempt})'

        symbol = self.executionStatus.symbol()
        color, rcol = '', ''
        if not monochrome:
            color = self.executionStatus.color()
            rcol = AnsiiColors.RESET

        extras_str = "".join("\n" + indent(e, '  ') for e in extras)
        return f'{color}[{symbol}] {name} ({duration_str}){extras_str}{rcol}'


def unwrap_caused_by(failures: List):

    inner_failures = []
    for failure in failures:
        m = failure.get('message', '')
        caused_by = failure.get('causedBy')
        if caused_by:
            m += f', caused by: {unwrap_caused_by(caused_by)}'
        if m:
            inner_failures.append(m)

    return " & ".join(inner_failures)


def prepare_inner_calls_string(
    name, calls: List['CallMetadata'], expand_completed=False, monochrome=False
):
    if len(calls) == 1:
        return calls[0].display(
            expand_completed=expand_completed,
            monochrome=monochrome,
        )

    collapsed_status = collapse_status_of_calls(calls)
    status = collapsed_status.symbol()
    color, rcol = '', ''
    if not monochrome:
        color = collapsed_status.color()
        rcol = AnsiiColors.RESET
    inner_calls = ''

    if len(calls) > 1 and not expand_completed:
        name += f' ({len(calls)} jobs)'

    if len(calls) > 0:
        starts = [c.start for c in calls if c.start is not None]
        finishes = [c.end for c in calls if c.end is not None]
        start, finish = None, None
        if starts:
            start = min(starts)
        if finishes:
            finish = max(finishes)
        name += f' ({get_readable_duration(get_seconds_duration_between_cromwell_dates(start, finish))})'

    if expand_completed or collapsed_status not in [
        ExecutionStatus.done,
        ExecutionStatus.succeeded,
    ]:
        inner_calls = "\n" + indent(
            '\n'.join(
                c.display(expand_completed=expand_completed, monochrome=monochrome)
                for c in calls
            ),
            '  ',
        )

    return f'{color}[{status}] {name}{inner_calls}{rcol}'


def collapse_status_of_calls(calls: List['CallMetadata']):
    collapsed = set(c.executionStatus for c in calls)
    if any(
        status in collapsed
        for status in [
            ExecutionStatus.preparing,
            ExecutionStatus.in_progress,
            ExecutionStatus.running,
        ]
    ):
        return ExecutionStatus.in_progress
    if ExecutionStatus.failed in collapsed:
        return ExecutionStatus.failed
    if len(collapsed) != 1:
        # hmm, don't know yet
        return ExecutionStatus.in_progress
    else:
        return list(collapsed)[0]


def get_seconds_duration_between_cromwell_dates(start, end):
    s, e = None, None
    if start:
        s = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S.%f%z").replace(
            tzinfo=None
        )
    if end:
        e = datetime.datetime.strptime(end, "%Y-%m-%dT%H:%M:%S.%f%z").replace(
            tzinfo=None
        )

    if not s:
        return None

    return int(((e or datetime.datetime.utcnow()) - s).total_seconds())


def get_readable_duration(seconds: int):
    """
    >>> get_readable_duration(86401)
    '1d:0h:0m:1s'

    >>> get_readable_duration(100)
    '1m:40s'

    >>> get_readable_duration(3)
    '3s'
    """
    if seconds is None:
        return "?"
    if seconds < 0:
        return "In the future..."

    if seconds < 1:
        return '<1s'

    intervals = [
        (365 * 86400, "y"),
        (7 * 86400, 'w'),
        (86400, 'd'),
        (3600, 'h'),
        (60, 'm'),
        (1, 's'),
    ]

    periods = []
    has_seen_value = False
    for interval, suffix in intervals:
        if interval > seconds and not has_seen_value:
            continue
        has_seen_value = True
        intervals, seconds = divmod(seconds, interval)
        periods.append(f'{intervals}{suffix}')

    # weird if we get to here, but sure
    return ":".join(periods)
