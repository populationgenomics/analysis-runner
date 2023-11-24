"""Helper functions to run Hail Query scripts on Dataproc from Hail Batch."""

import os
import re
import uuid
from shlex import quote
from typing import Collection, Dict, List, Optional, Tuple

import hailtop.batch as hb
from cpg_utils.config import AR_GUID_NAME, get_config, try_get_ar_guid
from cpg_utils.hail_batch import cpg_namespace

from analysis_runner.constants import GCLOUD_ACTIVATE_AUTH
from analysis_runner.git import (
    get_git_commit_ref_of_current_repository,
    get_git_default_remote,
    get_relative_path_from_git_root,
    get_repo_name_from_remote,
    prepare_git_job,
)

DEFAULT_HAIL_VERSION = '0.2.126'

_config = get_config()
ACCESS_LEVEL = _config['workflow']['access_level']
DATASET = _config['workflow']['dataset']
DATASET_GCP_PROJECT = _config['workflow']['dataset_gcp_project']
GCLOUD_CONFIG_SET_PROJECT = f'gcloud config set project {DATASET_GCP_PROJECT}'
PYFILES_DIR = '/tmp/pyfiles'
PYFILES_ZIP = 'pyfiles.zip'

DEFAULT_PACKAGES = [
    'analysis-runner',
    'bokeh',
    'botocore',
    'cpg-utils',
    'cpg-workflows',
    'gcsfs',
    'pyarrow',
    'metamist',
    'selenium>=3.8.0',
    'statsmodels',
    'cloudpathlib[all]',
    'gnomad',
]


def get_wheel_from_version(hail_version: str) -> str:
    """
    Use the wheel built on deploy of https://github.com/populationgenomics/hail
    The difference from the official build is the version of ElasticSearch:
    We use 8.x.x, and Hail is built for 7.x.x by default.
    """
    return f'gs://cpg-hail-ci/wheels/hail-{hail_version}-py3-none-any.whl'


def get_init_script_from_hail_version(hail_version: str) -> str:
    """
    Use a versioned init script to ensure that the best startup is used for
    a dataproc version
    """
    return f'gs://cpg-common-main/hail_dataproc/{hail_version}/'


def get_dataproc_driver_image_from_hail_version(hail_version: str) -> str:
    """
    Use a versioned driver image to ensure that the best startup is used for
    a dataproc version
    """
    return (
        f'australia-southeast1-docker.pkg.dev/analysis-runner/images/'
        f'dataproc:hail-{hail_version}'
    )


class DataprocCluster:
    """
    Helper class that represents a Dataproc cluster created within a Batch
    """

    def __init__(self, **kwargs):
        self._batch = kwargs.pop('batch')
        self._depends_on = kwargs.pop('depends_on', None)
        self._cluster_name = kwargs.get('cluster_name', None)
        self._region = kwargs.pop('region')
        self._cluster_id = None
        self._start_job = None
        self._stop_job = None
        self._hail_version = kwargs.pop('hail_version', DEFAULT_HAIL_VERSION)
        self._startup_params = kwargs
        self._stop_cluster = kwargs.pop('stop_cluster', True)

    def add_job(
        self,
        script: str,
        job_name: Optional[str] = None,
        pyfiles: Optional[List[str]] = None,
        attributes: Optional[Dict] = None,
        depends_on: Optional[List] = None,
    ) -> hb.batch.job.Job:
        """
        Create a job that submits the `script` to the cluster
        """
        if self._start_job is None:
            self._start_job, self._cluster_id = _add_start_job(
                batch=self._batch,
                attributes=attributes,
                region=self._region,
                hail_version=self._hail_version,
                **self._startup_params,
            )
            if self._depends_on:
                self._start_job.depends_on(*self._depends_on)

            if self._stop_cluster:
                self._stop_job = _add_stop_job(
                    batch=self._batch,
                    cluster_id=self._cluster_id,
                    job_name=job_name,
                    cluster_name=self._cluster_name,
                    attributes=attributes,
                    region=self._region,
                    hail_version=self._hail_version,
                )
                self._stop_job.depends_on(self._start_job)

        if self._cluster_id is None:
            raise ValueError('Cluster was not configured with an ID correctly')

        job = _add_submit_job(
            batch=self._batch,
            cluster_id=self._cluster_id,
            script=script,
            pyfiles=pyfiles,
            job_name=job_name,
            cluster_name=self._cluster_name,
            attributes=attributes,
            region=self._region,
            hail_version=self._hail_version,
        )
        job.depends_on(self._start_job)
        if self._stop_job:
            self._stop_job.depends_on(job)

        if depends_on:
            job.depends_on(*depends_on)

        return job


def setup_dataproc(  # pylint: disable=unused-argument,too-many-arguments
    batch: hb.Batch,
    max_age: str,
    num_workers: int = 2,
    num_secondary_workers: int = 0,
    region: str = 'australia-southeast1',
    worker_machine_type: Optional[str] = None,  # e.g. 'n1-highmem-8'
    master_boot_disk_size: Optional[int] = None,  # in GB
    worker_boot_disk_size: Optional[int] = None,  # in GB
    secondary_worker_boot_disk_size: Optional[int] = None,  # in GB
    packages: Optional[List[str]] = None,
    init: Optional[Collection[str]] = None,
    init_timeout: Optional[str] = None,
    vep: Optional[str] = None,
    requester_pays_allow_all: bool = False,
    depends_on: Optional[List[hb.batch.job.Job]] = None,
    job_name: Optional[str] = None,
    cluster_name: Optional[str] = None,
    scopes: Optional[List[str]] = None,
    labels: Optional[Dict[str, str]] = None,
    autoscaling_policy: Optional[str] = None,
    stop_cluster: Optional[bool] = True,
    install_default_packages: bool = True,
    hail_version: str = DEFAULT_HAIL_VERSION,
) -> DataprocCluster:
    """
    Adds jobs to the Batch that start and stop a Dataproc cluster, and returns
    a DataprocCluster object with an add_job() method, that inserts a job
    between start and stop.

    See the `hailctl` tool for information on the keyword parameters.

    `depends_on` can be used to enforce dependencies for the cluster start up.
    """
    return DataprocCluster(**locals())


def hail_dataproc_job(
    batch: hb.Batch,
    script: str,
    pyfiles: Optional[List[str]] = None,
    job_name: Optional[str] = None,
    attributes: Optional[Dict] = None,
    **kwargs,
) -> hb.batch.job.Job:
    """
    A legacy wrapper that adds a start, submit, and stop job altogether
    """
    kwargs['job_name'] = job_name
    cluster = setup_dataproc(batch, **kwargs)
    return cluster.add_job(script, job_name, pyfiles, attributes=attributes)


def _add_start_job(  # pylint: disable=too-many-arguments
    batch: hb.Batch,
    max_age: str,
    region: str,
    hail_version: str,
    num_workers: int = 2,
    num_secondary_workers: int = 0,
    autoscaling_policy: Optional[str] = None,
    worker_machine_type: Optional[str] = None,  # e.g. 'n1-highmem-8'
    master_boot_disk_size: Optional[int] = None,  # in GB
    worker_boot_disk_size: Optional[int] = None,  # in GB
    secondary_worker_boot_disk_size: Optional[int] = None,  # in GB
    packages: Optional[List[str]] = None,
    init: Optional[Collection[str]] = None,
    init_timeout: Optional[str] = None,
    vep: Optional[str] = None,
    requester_pays_allow_all: bool = False,
    cluster_name: Optional[str] = None,
    job_name: Optional[str] = None,
    scopes: Optional[List[str]] = None,
    labels: Optional[Dict[str, str]] = None,
    attributes: Optional[Dict] = None,
    install_default_packages: bool = True,
) -> Tuple[hb.batch.job.Job, str]:
    """
    Returns a Batch job which starts a Dataproc cluster, and the name of the cluster.
    The user is responsible for stopping the cluster.

    See the `hailctl` tool for information on the keyword parameters.
    """
    cluster_id = f'dp-{uuid.uuid4().hex[:20]}'

    job_name_prefix = f'{job_name}: s' if job_name else 'S'
    job_name = f'{job_name_prefix}tart Dataproc cluster'
    if cluster_name:
        job_name += f' "{cluster_name}"'
        cluster_name = re.sub(r'[^a-zA-Z0-9]+', '-', cluster_name.lower())
        # Cluster id can't be longer than 49 characters
        cluster_id = f'{cluster_id}-{cluster_name}'[:49]

    if labels is None:
        labels = {}
    labels['compute-category'] = 'dataproc'

    ar_guid = try_get_ar_guid()
    if ar_guid:
        labels[AR_GUID_NAME] = ar_guid

    labels_formatted = ','.join(f'{key}={value}' for key, value in labels.items())

    start_job = batch.new_job(name=job_name, attributes=attributes)
    start_job.image(get_dataproc_driver_image_from_hail_version(hail_version))
    start_job.command(GCLOUD_ACTIVATE_AUTH)
    start_job.command(GCLOUD_CONFIG_SET_PROJECT)

    # The spark-env property can be used to set environment variables in jobs that run
    # on the Dataproc cluster. We propagate some currently set environment variables
    # this way.
    spark_env = [f'spark-env:CPG_CONFIG_PATH={os.getenv("CPG_CONFIG_PATH")}']

    # Note that the options and their values must be separated by an equal sign.
    # Using a space will break some options like --label
    namespace = cpg_namespace(ACCESS_LEVEL)
    start_job_command = [
        'hailctl dataproc start',
        f'--region={region}',
        f'--service-account=dataproc-{ACCESS_LEVEL}@{DATASET_GCP_PROJECT}.iam.gserviceaccount.com',
        f'--max-age={max_age}',
        f'--num-workers={num_workers}',
        f'--num-secondary-workers={num_secondary_workers}',
        f'--properties="{",".join(spark_env)}"',
        f'--labels={labels_formatted}',
        f'--wheel={get_wheel_from_version(hail_version)}',
        f'--bucket=cpg-{DATASET}-{namespace}-tmp',
        f'--temp-bucket=cpg-{DATASET}-{namespace}-tmp',
    ]
    if worker_machine_type:
        start_job_command.append(f'--worker-machine-type={worker_machine_type}')
    if master_boot_disk_size:
        start_job_command.append(f'--master-boot-disk-size={master_boot_disk_size}')
    if worker_boot_disk_size:
        start_job_command.append(f'--worker-boot-disk-size={worker_boot_disk_size}')
    if secondary_worker_boot_disk_size:
        start_job_command.append(
            f'--secondary-worker-boot-disk-size={secondary_worker_boot_disk_size}'
        )
    _packages = []
    if install_default_packages:
        _packages.extend(DEFAULT_PACKAGES)
    if packages:
        _packages.extend(packages)
    if _packages:
        start_job_command.append(f'--packages={quote(",".join(_packages))}')
    _init = init
    if _init is None:
        # if init is None (by default), use the default init script.
        # pass an empty list to disable the default init script.
        _init = [get_init_script_from_hail_version(hail_version)]
    if _init:
        start_job_command.append(f'--init={",".join(_init)}')
    if init_timeout:
        start_job_command.append(f'--init_timeout={init_timeout}')
    if vep:
        start_job_command.append(f'--vep={vep}')
    if requester_pays_allow_all:
        start_job_command.append('--requester-pays-allow-all')
    if scopes:
        start_job_command.append(f'--scopes={",".join(scopes)}')
    if autoscaling_policy:
        start_job_command.append(f'--autoscaling-policy={autoscaling_policy}')

    start_job_command.append(cluster_id)

    start_job.command(' \\\n'.join(start_job_command))
    return start_job, cluster_id


def _add_submit_job(
    batch: hb.Batch,
    cluster_id: str,
    script: str,
    region: str,
    hail_version: str,
    pyfiles: Optional[List[str]] = None,
    job_name: Optional[str] = None,
    cluster_name: Optional[str] = None,
    attributes: Optional[Dict] = None,
) -> hb.batch.job.Job:
    """
    Returns a job that submits a script to the Dataproc cluster
    specified by `cluster_id`. It's the user's responsibility to start and stop
    that cluster with the `start_cluster` and `stop_cluster` functions
    """
    job_name_prefix = f'{job_name}: s' if job_name else 'S'
    job_name = f'{job_name_prefix}ubmit to Dataproc cluster'
    if cluster_name:
        job_name += f' "{cluster_name}"'

    main_job = batch.new_job(name=job_name, attributes=attributes)
    main_job.image(get_dataproc_driver_image_from_hail_version(hail_version))
    main_job.command(GCLOUD_ACTIVATE_AUTH)
    main_job.command(GCLOUD_CONFIG_SET_PROJECT)

    # Clone the repository to pass scripts to the cluster.
    prepare_git_job(
        job=main_job,
        repo_name=get_repo_name_from_remote(get_git_default_remote()),
        commit=get_git_commit_ref_of_current_repository(),
    )
    cwd = get_relative_path_from_git_root()
    if cwd:
        main_job.command(f'cd {quote(cwd)}')

    main_job.command(
        f'hailctl dataproc submit '
        f'--region={region} '
        + (f'--pyfiles {",".join(pyfiles)} ' if pyfiles else '')
        + f'{cluster_id} -- {script} '
    )
    return main_job


def _add_stop_job(
    batch: hb.Batch,
    cluster_id: str,
    region: str,
    hail_version: str,
    job_name: Optional[str] = None,
    cluster_name: Optional[str] = None,
    attributes: Optional[Dict] = None,
) -> hb.batch.job.Job:
    """
    Returns a job that stops the Dataproc cluster specified by `cluster_id`
    """
    job_name_prefix = f'{job_name}: s' if job_name else 'S'
    job_name = f'{job_name_prefix}top Dataproc cluster'
    if cluster_name:
        job_name += f' "{cluster_name}"'

    stop_job = batch.new_job(name=job_name, attributes=attributes)
    stop_job.always_run()  # Always clean up.
    stop_job.image(get_dataproc_driver_image_from_hail_version(hail_version))
    stop_job.command(GCLOUD_ACTIVATE_AUTH)
    stop_job.command(GCLOUD_CONFIG_SET_PROJECT)
    stop_job.command(f'hailctl dataproc stop --region={region} {cluster_id}')

    return stop_job
