"""Helper functions to run Hail Query scripts on Dataproc from Hail Batch."""

import os
import uuid
from typing import Optional, List, Dict, Tuple
import hailtop.batch as hb
from analysis_runner.git import (
    get_git_default_remote,
    get_git_commit_ref_of_current_repository,
    get_relative_script_path_from_git_root,
    get_repo_name_from_remote,
)


DATAPROC_IMAGE = (
    'australia-southeast1-docker.pkg.dev/analysis-runner/images/dataproc:hail-0.2.73'
)
GCLOUD_AUTH = 'gcloud -q auth activate-service-account --key-file=/gsa-key/key.json'
GCLOUD_PROJECT = f'gcloud config set project {os.getenv("DATASET_GCP_PROJECT")}'
DATAPROC_REGION = 'gcloud config set dataproc/region australia-southeast1'
PYFILES_DIR = '/tmp/pyfiles'
PYFILES_ZIP = 'pyfiles.zip'


class DataprocCluster:
    """
    Helper class that represents a Dataproc cluster created within a Batch
    """

    def __init__(
        self,
        batch: hb.Batch,
        start_job: hb.batch.job.Job,
        stop_job: hb.batch.job.Job,
        cluster_name: str,
    ):
        self.batch = batch
        self.start_job = start_job
        self.stop_job = stop_job
        self.cluster_name = cluster_name

    def add_job(
        self,
        script: str,
        job_name: Optional[str] = None,
        pyfiles: Optional[List[str]] = None,
    ) -> hb.batch.job.Job:
        """
        Create a job that submits the `script` to the cluster
        """
        job = _add_submit_job(
            batch=self.batch,
            cluster_name=self.cluster_name,
            script=script,
            job_name=job_name,
            pyfiles=pyfiles,
        )
        job.depends_on(self.start_job)
        self.stop_job.depends_on(job)
        return job


def hail_dataproc(
    batch: hb.Batch,
    *,
    max_age: str,
    num_workers: int = 2,
    num_secondary_workers: int = 0,
    worker_machine_type: Optional[str] = None,  # e.g. 'n1-highmem-8'
    worker_boot_disk_size: Optional[int] = None,  # in GB
    secondary_worker_boot_disk_size: Optional[int] = None,  # in GB
    packages: Optional[List[str]] = None,
    init: Optional[List[str]] = None,
    vep: Optional[str] = None,
    requester_pays_allow_all: bool = True,
    depends_on: Optional[List[hb.batch.job.Job]] = None,
    job_name: Optional[str] = None,
    scopes: Optional[List[str]] = None,
    labels: Optional[Dict[str, str]] = None,
) -> DataprocCluster:
    """
    Adds jobs to the batch that start and stop a Dataproc cluster, and returns
    a DataprocCluster object with a submit() method that allows to add jobs into
    this cluster that submit Query scripts.

    See the `hailctl` tool for information on the keyword parameters.

    depends_on can be used to enforce dependencies for the new job.
    """

    start_job, cluster_name = _add_start_job(
        batch=batch,
        max_age=max_age,
        num_workers=num_workers,
        num_secondary_workers=num_secondary_workers,
        worker_machine_type=worker_machine_type,
        worker_boot_disk_size=worker_boot_disk_size,
        secondary_worker_boot_disk_size=secondary_worker_boot_disk_size,
        packages=packages,
        init=init,
        vep=vep,
        requester_pays_allow_all=requester_pays_allow_all,
        scopes=scopes,
        labels=labels,
        job_name=job_name,
    )
    if depends_on:
        start_job.depends_on(*depends_on)

    stop_job = _add_stop_job(
        batch=batch,
        cluster_name=cluster_name,
        job_name=job_name,
    )
    stop_job.depends_on(start_job)

    return DataprocCluster(
        batch=batch, start_job=start_job, stop_job=stop_job, cluster_name=cluster_name
    )


def hail_dataproc_job(
    batch: hb.Batch,
    script: str,
    *,
    max_age: str,
    num_workers: int = 2,
    num_secondary_workers: int = 0,
    worker_machine_type: Optional[str] = None,  # e.g. 'n1-highmem-8'
    worker_boot_disk_size: Optional[int] = None,  # in GB
    secondary_worker_boot_disk_size: Optional[int] = None,  # in GB
    packages: Optional[List[str]] = None,
    pyfiles: Optional[List[str]] = None,
    init: Optional[List[str]] = None,
    vep: Optional[str] = None,
    requester_pays_allow_all: bool = True,
    depends_on: Optional[List[hb.batch.job.Job]] = None,
    job_name: Optional[str] = None,
    scopes: Optional[List[str]] = None,
    labels: Optional[Dict[str, str]] = None,
) -> hb.batch.job.Job:
    """Returns a Batch job which starts a Dataproc cluster, submits a Hail
    Query script to it, and stops the cluster. See the `hailctl` tool for
    information on the keyword parameters. depends_on can be used to enforce
    dependencies for the new job."""

    start_job, cluster_name = _add_start_job(
        batch=batch,
        max_age=max_age,
        num_workers=num_workers,
        num_secondary_workers=num_secondary_workers,
        worker_machine_type=worker_machine_type,
        worker_boot_disk_size=worker_boot_disk_size,
        secondary_worker_boot_disk_size=secondary_worker_boot_disk_size,
        packages=packages,
        init=init,
        vep=vep,
        requester_pays_allow_all=requester_pays_allow_all,
        scopes=scopes,
        labels=labels,
        job_name=job_name,
    )
    if depends_on:
        start_job.depends_on(*depends_on)

    main_job = _add_submit_job(
        batch=batch,
        cluster_name=cluster_name,
        script=script,
        job_name=job_name,
        pyfiles=pyfiles,
    )
    main_job.depends_on(start_job)

    stop_job = _add_stop_job(
        batch=batch,
        cluster_name=cluster_name,
        job_name=job_name,
    )
    stop_job.depends_on(main_job)

    return stop_job


def _add_start_job(
    batch: hb.Batch,
    *,
    max_age: str,
    num_workers: int = 2,
    num_secondary_workers: int = 0,
    worker_machine_type: Optional[str] = None,  # e.g. 'n1-highmem-8'
    worker_boot_disk_size: Optional[int] = None,  # in GB
    secondary_worker_boot_disk_size: Optional[int] = None,  # in GB
    packages: Optional[List[str]] = None,
    init: Optional[List[str]] = None,
    vep: Optional[str] = None,
    requester_pays_allow_all: bool = True,
    job_name: Optional[str] = None,
    scopes: Optional[List[str]] = None,
    labels: Optional[Dict[str, str]] = None,
) -> Tuple[hb.batch.job.Job, str]:
    """
    Returns a Batch job which starts a Dataproc cluster, and the name of the cluster.
    The user is respondible for stopping the cluster.

    See the `hailctl` tool for information on the keyword parameters.
    """
    job_name_prefix = f'{job_name}: ' if job_name else ''
    job_name = f'{job_name_prefix}start Dataproc cluster'

    cluster_name = f'dataproc-{uuid.uuid4().hex}'

    if labels is None:
        labels = {}
    labels['compute-category'] = 'dataproc'
    labels_formatted = ','.join(f'{key}={value}' for key, value in labels.items())

    start_job = batch.new_job(name=job_name)
    start_job.image(DATAPROC_IMAGE)
    start_job.command(GCLOUD_AUTH)
    start_job.command(GCLOUD_PROJECT)
    start_job.command(DATAPROC_REGION)

    # The spark-env property can be used to set environment variables in jobs that run
    # on the Dataproc cluster. We propagate some currently set environment variables
    # this way.
    spark_env = []
    for env_var in 'DATASET', 'ACCESS_LEVEL', 'OUTPUT':
        value = os.getenv(env_var)
        assert value, f'environment variable "{env_var}" is not set'
        spark_env.append(f'spark-env:{env_var}={value}')

    # Note that the options and their values must be separated by an equal sign. Using a space will break some options like --label
    start_job_command = [
        'hailctl dataproc start',
        f'--service-account=dataproc-{os.getenv("ACCESS_LEVEL")}@{os.getenv("DATASET_GCP_PROJECT")}.iam.gserviceaccount.com',
        f'--max-age={max_age}',
        f'--num-workers={num_workers}',
        f'--num-secondary-workers={num_secondary_workers}',
        f'--properties="{",".join(spark_env)}"',
        f'--labels={labels_formatted}',
    ]
    if worker_machine_type:
        start_job_command.append(f'--worker-machine-type={worker_machine_type}')
    if worker_boot_disk_size:
        start_job_command.append(f'--worker-boot-disk-size={worker_boot_disk_size}')
    if secondary_worker_boot_disk_size:
        start_job_command.append(
            f'--secondary-worker-boot-disk-size={secondary_worker_boot_disk_size}'
        )
    if packages:
        start_job_command.append(f'--packages={",".join(packages)}')
    if init:
        start_job_command.append(f'--init={",".join(init)}')
    if vep:
        start_job_command.append(f'--vep={vep}')
    if requester_pays_allow_all:
        start_job_command.append(f'--requester-pays-allow-all')
    if scopes:
        start_job_command.append(f'--scopes={",".join(scopes)}')
    start_job_command.append(cluster_name)

    start_job.command(' '.join(start_job_command))
    return start_job, cluster_name


def _add_submit_job(
    batch: hb.Batch,
    cluster_name: str,
    script: str,
    job_name: Optional[str] = None,
    pyfiles: Optional[List[str]] = None,
) -> hb.batch.job.Job:
    """
    Returns a job that submits a script to the Dataproc cluster
    specified by `cluster_name`. It's the user's responsibility to start and stop
    that cluster with the `start_cluster` and `stop_cluster` functions
    """
    job_name_prefix = f'{job_name}: ' if job_name else ''
    job_name = f'{job_name_prefix}submit to Dataproc cluster'

    main_job = batch.new_job(name=job_name)
    main_job.image(DATAPROC_IMAGE)
    main_job.command(GCLOUD_AUTH)
    main_job.command(GCLOUD_PROJECT)
    main_job.command(DATAPROC_REGION)

    # Clone the repository to pass scripts to the cluster.
    git_remote = get_git_default_remote()
    git_sha = get_git_commit_ref_of_current_repository()
    git_dir = get_relative_script_path_from_git_root('')
    repo_name = get_repo_name_from_remote(git_remote)

    main_job.command(f'git clone --recurse-submodules {git_remote} {repo_name}')
    main_job.command(f'cd {repo_name}')
    main_job.command(f'git checkout {git_sha}')
    main_job.command(f'git submodule update')
    main_job.command(f'cd ./{git_dir}')

    if pyfiles:
        main_job.command(f'mkdir {PYFILES_DIR}')
        main_job.command(f'cp -r {" ".join(pyfiles)} {PYFILES_DIR}')
        main_job.command(f'cd {PYFILES_DIR}')
        main_job.command(f'zip -r {PYFILES_ZIP} .')
        main_job.command(f'cd -')

    main_job.command(
        f'hailctl dataproc submit '
        + (f'--pyfiles {PYFILES_DIR}/{PYFILES_ZIP} ' if pyfiles else '')
        + f'{cluster_name} {script} '
    )
    return main_job


def _add_stop_job(
    batch: hb.Batch,
    cluster_name: str,
    job_name: Optional[str] = None,
) -> hb.batch.job.Job:
    """
    Returns a job that stops the Dataproc cluster specified by `cluster_name`
    """
    job_name_prefix = f'{job_name}: ' if job_name else ''
    job_name = f'{job_name_prefix}stop Dataproc cluster'

    stop_job = batch.new_job(name=job_name)
    stop_job.always_run()  # Always clean up.
    stop_job.image(DATAPROC_IMAGE)
    stop_job.command(GCLOUD_AUTH)
    stop_job.command(GCLOUD_PROJECT)
    stop_job.command(DATAPROC_REGION)
    stop_job.command(f'hailctl dataproc stop {cluster_name}')

    return stop_job
