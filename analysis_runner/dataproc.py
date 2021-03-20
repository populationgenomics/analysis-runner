"""Helper functions to run Hail Query scripts on Dataproc from Hail Batch."""

import os
import uuid
import subprocess
from typing import Optional, List
import hailtop.batch as hb

DRIVER_IMAGE = (
    'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:'
    '7f1a676f0b1e734981878576f6f091689e7d71c1-hail-0.2.64.dev529856899024'
)
REGION = 'australia-southeast1'
GCLOUD_AUTH = 'gcloud -q auth activate-service-account --key-file=/gsa-key/key.json'
GCLOUD_PROJECT = 'gcloud config set project hail-295901'
PYFILES_DIR = '/tmp/pyfiles'
PYFILES_ZIP = 'pyfiles.zip'


def hail_dataproc_job(
    batch: hb.Batch,
    script: str,
    *,
    max_age: str,
    num_workers: int = 2,
    num_secondary_workers: int = 0,
    packages: Optional[List[str]] = None,
    pyfiles: Optional[List[str]] = None,
    init: Optional[List[str]] = None,
    vep: Optional[str] = None,
    requester_pays_allow_all: bool = False,
    depends_on: Optional[List[hb.batch.job.Job]] = None,
) -> hb.batch.job.Job:
    """Returns a Batch job which starts a Dataproc cluster, submits a Hail
    Query script to it, and stops the cluster. See the `hailctl` tool for
    information on the keyword parameters. depends_on can be used to enforce
    dependencies for the new job."""

    cluster_name = f'dataproc-{uuid.uuid4().hex}'

    start_job = batch.new_job(name='start Dataproc cluster')
    if depends_on:
        for dependency in depends_on:
            start_job.depends_on(dependency)
    start_job.image(DRIVER_IMAGE)
    start_job.command(GCLOUD_AUTH)
    start_job.command(GCLOUD_PROJECT)
    start_job.command(
        f'hailctl dataproc start --region {REGION} --service-account='
        f'$(gcloud config list account --format "value(core.account)") '
        f'--max-age {max_age} --num-workers {num_workers} '
        f'--num-secondary-workers {num_secondary_workers} '
        + (f'--packages {",".join(packages)} ' if packages else '')
        + (f'--init {",".join(init)} ' if init else '')
        + (f'--vep {vep}' if vep else '')
        + (f'--requester-pays-allow-all ' if requester_pays_allow_all else '')
        + f'{cluster_name}'
    )

    main_job = batch.new_job(name='main')
    main_job.depends_on(start_job)
    main_job.image(DRIVER_IMAGE)
    main_job.command(GCLOUD_AUTH)
    main_job.command(GCLOUD_PROJECT)

    # Clone the repository to pass scripts to the cluster.
    git_repo = subprocess.check_output(
        ['git', 'config', '--get', 'remote.origin.url'], encoding='UTF-8'
    ).strip()
    git_sha = subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], encoding='UTF-8'
    ).strip()
    git_dir = subprocess.check_output(
        ['git', 'rev-parse', '--show-toplevel'], encoding='UTF-8'
    ).strip()
    git_subdir = os.getcwd()[len(git_dir) + 1 :]

    main_job.command(f'git clone {git_repo}')
    main_job.command(f'cd repo')
    main_job.command(f'git checkout {git_sha}')
    main_job.command(f'cd ./{git_subdir}')

    if pyfiles:
        os.mkdir(PYFILES_DIR)
        subprocess.check_call(['cp', '-r'] + pyfiles + [PYFILES_DIR])
        subprocess.check_call(['zip', '-r', PYFILES_ZIP, '.'], cwd=PYFILES_DIR)

    main_job.command(
        f'hailctl dataproc submit --region {REGION} '
        + (f'--pyfiles {PYFILES_DIR}/{PYFILES_ZIP}' if pyfiles else '')
        + f'{cluster_name} {script} '
    )

    stop_job = batch.new_job(name='stop Dataproc cluster')
    stop_job.depends_on(main_job)
    stop_job.always_run()  # Always clean up.
    stop_job.image(DRIVER_IMAGE)
    stop_job.command(GCLOUD_AUTH)
    stop_job.command(GCLOUD_PROJECT)
    stop_job.command(f'hailctl dataproc stop --region {REGION} {cluster_name}')

    return stop_job
