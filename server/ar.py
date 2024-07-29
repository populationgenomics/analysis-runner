# ruff: noqa: E402
import dataclasses
import datetime
import json
from shlex import quote

from aiohttp import web
from util import (
    PUBSUB_TOPIC,
    _get_hail_version,
    add_environment_variables,
    check_dataset_and_group,
    generate_ar_guid,
    get_analysis_runner_metadata,
    get_and_check_cloud_environment,
    get_and_check_commit,
    get_and_check_image,
    get_and_check_repository,
    get_and_check_script,
    get_baseline_run_config,
    get_email_from_request,
    get_hail_token,
    get_server_config,
    publisher,
    validate_output_dir,
    write_config,
)

import hailtop.batch as hb

from cpg_utils.config import AR_GUID_NAME, update_dict
from cpg_utils.git import guess_script_github_url_from
from cpg_utils.hail_batch import (
    prepare_git_job,
    remote_tmpdir,
    run_batch_job_and_print_url,
)


@dataclasses.dataclass
class AnalysisRunnerJobArgs:
    output: str
    dataset: str
    cloud_environment: str
    access_level: str
    description: str
    config: dict

    # repo specific things
    repo: str | None
    commit: str | None
    branch: str | None
    cwd: str | None

    # job specific things
    script: list[str]
    image: str
    cpu: int
    memory: str | None
    storage: str | None
    environment_variables: dict | None
    preemptible: bool

    # job submission
    hail_token: str
    gcp_project: str

    def is_test(self) -> bool:
        return self.access_level == 'test'

    def get_batch_attributes(self) -> dict:
        attributes = {}
        if self.repo and self.commit:
            attributes['repo'] = self.repo
            attributes['commit'] = self.commit
        if self.branch:
            attributes['branch'] = self.branch

        return attributes

    def get_batch_comments(self) -> list[str]:
        comments = []
        if self.branch:
            comments.append(f'BRANCH: {self.branch}')

        if self.repo and self.commit:
            script_url = guess_script_github_url_from(
                repo=self.repo,
                commit=self.commit,
                script=self.script,
                cwd=self.cwd,
            )
            if script_url:
                comments.append(f'URL: {script_url}')

        return comments


def add_analysis_runner_routes(routes: web.RouteTableDef):
    """Add cromwell route(s) to 'routes' flask API"""

    @routes.post('/')
    async def index(request: web.Request) -> web.Response:
        """Main entry point, responds to the web root."""

        email = get_email_from_request(request)
        # When accessing a missing entry in the params dict, the resulting KeyError
        # exception gets translated to a Bad Request error in the try block.
        params = await request.json()

        ar_guid = generate_ar_guid()
        server_config = get_server_config()
        job_config = prepare_inputs_from_request_json(
            params,
            email=email,
            server_config=server_config,
        )

        hail_bucket = f'cpg-{job_config.dataset}-hail'
        backend = hb.ServiceBackend(
            billing_project=job_config.dataset,
            remote_tmpdir=remote_tmpdir(hail_bucket),
            token=job_config.hail_token,
        )

        # This metadata dictionary gets stored in the metadata bucket, at the output_dir location.
        hail_version = await _get_hail_version(environment=job_config.cloud_environment)
        timestamp = datetime.datetime.now().astimezone().isoformat()

        # Prepare the job's configuration and write it to a blob.

        run_config = get_baseline_run_config(
            ar_guid=ar_guid,
            environment=job_config.cloud_environment,
            gcp_project_id=job_config.gcp_project,
            dataset=job_config.dataset,
            access_level=job_config.access_level,
            output_prefix=job_config.output,
            driver=job_config.image,
        )
        if user_config := job_config.config:  # Update with user-specified configs.
            update_dict(run_config, user_config)

        config_path = write_config(
            ar_guid=ar_guid,
            config=run_config,
            environment=job_config.cloud_environment,
        )

        user_name = email.split('@')[0]
        job_provenance = job_config.image
        if job_config.repo:
            job_provenance = f'{job_config.repo}:{job_config.commit}'

        batch_name = f'{user_name} {job_provenance}/{" ".join(job_config.script)}'

        metadata = get_analysis_runner_metadata(
            ar_guid=ar_guid,
            name=batch_name,
            timestamp=timestamp,
            dataset=job_config.dataset,
            user=email,
            access_level=job_config.access_level,
            repo=job_config.repo,
            commit=job_config.commit,
            script=' '.join(job_config.script),
            description=job_config.description,
            output_prefix=job_config.output,
            hailVersion=hail_version,
            driver_image=job_config.image,
            config_path=config_path,
            cwd=job_config.cwd,
            environment=job_config.cloud_environment,
        )

        extra_batch_params = {}

        if job_config.cloud_environment == 'gcp':
            extra_batch_params['requester_pays_project'] = job_config.gcp_project

        attributes = {
            AR_GUID_NAME: ar_guid,
            'author': user_name,
        }

        batch = hb.Batch(
            backend=backend,
            name=batch_name,
            **extra_batch_params,
            attributes=attributes,
        )

        _job = prepare_job_from_config(
            batch=batch,
            job_config=job_config,
            config_path=config_path,
        )

        url = run_batch_job_and_print_url(
            batch,
            wait=params.get('wait', False),
            environment=job_config.cloud_environment,
        )

        # Publish the metadata to Pub/Sub.
        metadata['batch_url'] = url
        publisher.publish(PUBSUB_TOPIC, json.dumps(metadata).encode('utf-8')).result()

        return web.Response(text=f'{url}/jobs/1\n')


def prepare_inputs_from_request_json(
    params: dict,
    email: str,
    server_config: dict,
) -> AnalysisRunnerJobArgs:
    output_prefix = validate_output_dir(params['output'])
    dataset = params['dataset']
    access_level = params['accessLevel']

    cloud_environment = get_and_check_cloud_environment(params)

    dataset_config = check_dataset_and_group(
        server_config=server_config,
        environment=cloud_environment,
        dataset=dataset,
        email=email,
    )
    environment_config = dataset_config.get(cloud_environment, {})

    gcp_project_id = environment_config.get('projectId')

    is_test = access_level == 'test'

    repo = get_and_check_repository(params, dataset_config)
    commit = get_and_check_commit(params, repo)

    return AnalysisRunnerJobArgs(
        dataset=dataset,
        output=output_prefix,
        cloud_environment=cloud_environment,
        access_level=access_level,
        description=params['description'],
        config=params.get('config'),
        # repo specific
        repo=get_and_check_repository(params, dataset_config),
        commit=commit,
        branch=params.get('branch'),
        cwd=params.get('cwd'),
        script=get_and_check_script(params),
        # job specific
        image=get_and_check_image(params, is_test),
        cpu=params.get('cpu', 1),
        memory=params.get('memory'),
        storage=params.get('storage'),
        environment_variables=params.get('environmentVariables'),
        preemptible=params.get('preemptible', True),
        # other
        hail_token=get_hail_token(
            dataset=dataset,
            environment_config=environment_config,
            access_level=access_level,
        ),
        gcp_project=gcp_project_id,
    )


def prepare_job_from_config(
    batch: hb.Batch,
    job_config: AnalysisRunnerJobArgs,
    config_path: str,
) -> hb.batch.job.Job:

    job = batch.new_job(name='driver')
    job.env('CPG_CONFIG_PATH', config_path)

    # add comments
    comments = job_config.get_batch_comments()
    if comments:
        job.command('\n'.join(f'echo {quote(comment)}' for comment in comments))

    job.image(job_config.image)
    if job_config.cpu:
        job.cpu(job_config.cpu)
    if job_config.storage:
        job.storage(job_config.storage)
    if job_config.memory:
        job.memory(job_config.memory)
    job._preemptible = job_config.preemptible  # noqa: SLF001

    if job_config.environment_variables:
        add_environment_variables(job, job_config.environment_variables)

    if job_config.repo:
        prepare_job_with_repo(job, job_config)

    # Finally, run the script.
    escaped_script = ' '.join(quote(s) for s in job_config.script if s)
    job.command(escaped_script)

    return job


def prepare_job_with_repo(job: hb.batch.job.BashJob, config: AnalysisRunnerJobArgs):

    if not config.repo or not config.commit:
        raise ValueError('Internal error: missing repo or commit')

    job = prepare_git_job(
        job=job,
        repo_name=config.repo,
        commit=config.commit,
        is_test=config.is_test(),
    )

    if config.cwd:
        job.command(f'cd {quote(config.cwd)}')
    script = config.script
    job.command(f'which {quote(script[0])} || chmod +x {quote(script[0])}')
