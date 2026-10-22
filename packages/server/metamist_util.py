import asyncio
import json
import logging
from typing import Any

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from cpg_utils.config import AR_GUID_NAME
from metamist.apis import AnalysisRunnerApi
from metamist.exceptions import ServiceException

# test access level runs are logged in the dataset's test project in Metamist
TEST_PROJECT_SUFFIX = '-test'


def get_metamist_project(dataset: str, access_level: str) -> str:
    """Get the Metamist project given a dataset name and access level."""
    if access_level == 'test':
        return dataset + TEST_PROJECT_SUFFIX
    return dataset


def get_dataset(metamist_project: str, access_level: str) -> str:
    """Get the dataset of an analysis runner entry in this Metamist project."""
    if access_level == 'test':
        return metamist_project.removesuffix(TEST_PROJECT_SUFFIX)
    return metamist_project


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(
        initial=1,
        jitter=1,
    ),
    retry=retry_if_exception_type(ServiceException),
)
async def _create_metamist_log(**kwargs: Any) -> None:
    await asyncio.to_thread(AnalysisRunnerApi().create_analysis_runner_log, **kwargs)


async def log_submission_to_metamist(metadata: dict) -> None:
    """
    Record the submission in Metamist. Retry on errors
    """
    project = get_metamist_project(metadata['dataset'], metadata['accessLevel'])

    ar_guid = str(metadata.get(AR_GUID_NAME))
    try:
        await _create_metamist_log(
            project=project,
            ar_guid=ar_guid,
            access_level=str(metadata.get('accessLevel')),
            repository=str(metadata.get('repo')),
            commit=str(metadata.get('commit')),
            script=str(metadata.get('script')),
            description=str(metadata.get('description')),
            driver_image=str(metadata.get('driverImage')),
            config_path=(
                str(metadata['configPath'])
                if metadata.get('configPath') is not None
                else None
            ),
            environment=str(metadata.get('environment')),
            batch_url=str(metadata.get('batch_url')),
            submitting_user=str(metadata.get('user')),
            output_path=str(metadata.get('output')),
            request_body=metadata.get('meta', {}) or {},
            hail_version=str(metadata.get('hailVersion')),
            cwd=str(metadata.get('cwd')),
        )
    except Exception:
        logging.exception(
            f'Failed to create analysis-runner log in metamist for {ar_guid}, '
            f'AR record: {json.dumps(metadata, default=str)}',
        )
        return

    logging.info(f'Created analysis-runner log in metamist for {ar_guid}')
