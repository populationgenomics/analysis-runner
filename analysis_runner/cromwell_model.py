# flake8: noqa
# pylint: skip-file

from textwrap import indent
from typing import List
import datetime

from tabulate import tabulate


class WorkflowMetadataModel:
    def __init__(
        self,
        workflowName=None,
        workflowProcessingEvents=None,
        metadataSource=None,
        actualWorkflowLanguageVersion=None,
        submittedFiles=None,
        calls: List['CallMetadata'] = None,
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
        self.status = status
        self.end = end
        self.start = start

    @staticmethod
    def parse(d):
        new_d = {**d}
        calls_d = new_d.pop('calls')
        calls = [
            CallMetadata.parse({'name': name, **call})
            for name, sublist in calls_d.items()
            for call in sublist
        ]

        return WorkflowMetadataModel(calls=calls, **new_d)

    def display(self):

        duration_seconds = get_seconds_duration_between(self.start, self.end)
        duration_str = get_readable_duration(duration_seconds)

        headers = [
            ('Workflow ID', self.id),
            ('Status', self.status),
            ('Start', self.start),
            ('End', self.end),
            ('Duration', duration_str),
        ]

        calls = ''.join('\n' + indent(c.display(), '  ') for c in self.calls)
        header = tabulate(headers)
        return f"""
{header}
Jobs:
{calls}
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
        calls=None,
        **kwargs,
    ):
        self.name = name
        self.executionStatus = executionStatus
        self.stdout = stdout
        self.backendStatus = backendStatus
        self.compressedDockerSize = compressedDockerSize
        self.commandLine = commandLine
        self.shardIndex = shardIndex
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

        # safety
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    @staticmethod
    def parse(d):
        new_d = {**d}
        calls = None
        if 'subWorkflowMetadata' in new_d:
            calls_d = new_d.pop('subWorkflowMetadata').get('calls')
            calls = [
                CallMetadata(name=name, **call)
                for name, sublist in calls_d.items()
                for call in sublist
            ]
            calls = sorted(calls, key=lambda c: c.start)
        return CallMetadata(calls=calls, **new_d)

    def display(self, expand_completed=True):
        duration_str = get_readable_duration(
            get_seconds_duration_between(self.start, self.end)
        )

        extras = []
        if (self.executionStatus != 'Done' or expand_completed) and self.calls:
            extras.extend(call.display() for call in self.calls)

        name = self.name

        if self.shardIndex is not None:
            name += f' (shard-{self.shardIndex})'

        symbol = symbol_for_cromwell_status(self.executionStatus)
        extras_str = "".join("\n" + indent(e, '  ') for e in extras)
        return f'[{symbol}] {name} ({duration_str}){extras_str}'


def symbol_for_cromwell_status(status: str):
    status = status.lower()
    if status == 'done':
        return 'o'

    print(f'Unrecognised cromwell status: {status}')
    return f"? ({status})"


def get_seconds_duration_between(start, end):
    s, e = None, None
    if start:
        s = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S.%f%z")
    if end:
        e = datetime.datetime.strptime(end, "%Y-%m-%dT%H:%M:%S.%f%z")

    return int(((e or datetime.datetime.utcnow()) - s).total_seconds())


def get_readable_duration(seconds: int):
    """
    >>> get_readable_duration(86401)
    '1d:0h:0m:1s'

    >>> get_readable_duration(100)
    '1m:40s'

    >>> get_readable_duration(3)
    'Just now'
    """

    if seconds < 0:
        return "In the future..."
    if seconds < 5:
        return "Just now"

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
