"""Helper functions to run Hail Query scripts on Dataproc from Hail Batch."""

import os
import json
import uuid
from typing import Optional, List, Dict
import hailtop.batch as hb
from analysis_runner.git import (
    get_git_default_remote,
    get_git_commit_ref_of_current_repository,
    get_relative_script_path_from_git_root,
    get_repo_name_from_remote,
)

DATAPROC_IMAGE = (
    'australia-southeast1-docker.pkg.dev/analysis-runner/images/dataproc:hail-0.2.63'
)
GCLOUD_AUTH = 'gcloud -q auth activate-service-account --key-file=/gsa-key/key.json'
GCLOUD_PROJECT = f'gcloud config set project {os.getenv("DATASET_GCP_PROJECT")}'
DATAPROC_REGION = 'gcloud config set dataproc/region australia-southeast1'
PYFILES_DIR = '/tmp/pyfiles'
PYFILES_ZIP = 'pyfiles.zip'


def hail_dataproc_job(
    batch: hb.Batch,
    script: str,
    *,
    max_age: str,
    num_workers: int = 2,
    num_secondary_workers: int = 0,
    labels: Dict[str],
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
) -> hb.batch.job.Job:
    """Returns a Batch job which starts a Dataproc cluster, submits a Hail
    Query script to it, and stops the cluster. See the `hailctl` tool for
    information on the keyword parameters. depends_on can be used to enforce
    dependencies for the new job."""

    cluster_name = f'dataproc-{uuid.uuid4().hex}'

    # Format labels
    labels_string = re.sub('"|{|}| ', "", json.dumps(labels))
    labels_formatted = re.sub(":", "=", labels_string)

    job_name_prefix = f'{job_name}: ' if job_name else ''
    start_job = batch.new_job(name=f'{job_name_prefix}start Dataproc cluster')
    if depends_on:
        for dependency in depends_on:
            start_job.depends_on(dependency)
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

    start_job_command = [
        'hailctl dataproc start',
        f'--service-account=dataproc-{os.getenv("ACCESS_LEVEL")}@{os.getenv("DATASET_GCP_PROJECT")}.iam.gserviceaccount.com',
        f'--max-age={max_age}',
        f'--num-workers={num_workers}',
        f'--num-secondary-workers={num_secondary_workers}',
        f'--properties="{",".join(spark_env)}"',
        f'--labels {labels_formatted}',
    ]
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

    main_job = batch.new_job(name=f'{job_name_prefix}main')
    main_job.depends_on(start_job)
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

    stop_job = batch.new_job(name=f'{job_name_prefix}stop Dataproc cluster')
    stop_job.depends_on(main_job)
    stop_job.always_run()  # Always clean up.
    stop_job.image(DATAPROC_IMAGE)
    stop_job.command(GCLOUD_AUTH)
    stop_job.command(GCLOUD_PROJECT)
    stop_job.command(DATAPROC_REGION)
    stop_job.command(f'hailctl dataproc stop {cluster_name}')

    return stop_job
