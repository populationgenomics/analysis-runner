"""
Exports 'add_cromwell_routes', to add the following route to a flask API:
    POST /cromwell: Posts a workflow to a cromwell_url
"""

import dataclasses
import json
import os
from datetime import datetime
from shlex import quote

import requests
from aiohttp import web
from util import (
    PUBSUB_TOPIC,
    check_allowed_repos,
    check_dataset_and_group,
    generate_ar_guid,
    get_analysis_runner_metadata,
    get_and_check_commit,
    get_and_check_image,
    get_and_check_repository,
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
from cpg_utils.constants import CROMWELL_URL
from cpg_utils.cromwell import get_cromwell_oauth_token, run_cromwell_workflow
from cpg_utils.git import guess_script_github_url_from
from cpg_utils.hail_batch import (
    prepare_git_job,
    remote_tmpdir,
    run_batch_job_and_print_url,
)


@dataclasses.dataclass
class CromwellJobArgs:
    dataset: str
    access_level: str
    cloud_environment: str
    description: str
    labels: dict[str, str]

    output: str
    image: str

    # repo checkout
    branch: str | None
    repo: str
    commit: str
    cwd: str

    # workflow specific
    workflow: str
    dependencies: list[str]
    inputs: dict
    input_jsons: list[str]

    # job submission
    gcp_project: str
    hail_token: str

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
                script=[self.workflow],
                cwd=self.cwd,
            )
            if script_url:
                comments.append(f'URL: {script_url}')

        return comments


def add_cromwell_routes(routes: web.RouteTableDef):
    """Add cromwell route(s) to 'routes' flask API"""

    @routes.post('/cromwell')
    async def cromwell(request: web.Request) -> web.Response:
        """
        Checks out a repo, and POSTs the designated workflow to the cromwell server.
        Returns a hail batch link, eg: 'batch.hail.populationgenomics.org.au/batches/{batch}'
        ---
        :param output: string
        :param dataset: string
        :param accessLevel: string
        :param repo: string
        :param commit: string
        :param cwd: string (to set the working directory, relative to the repo root)
        :param description: string (Description of the workflow to run)
        :param workflow: string (the relative path of the workflow (from the cwd))
        :param input_json_paths: List[string] (the relative path to an inputs.json (from the cwd). Currently only supports one inputs.json)
        :param dependencies: List[string] (An array of directories (/ files) to zip for the '-p / --tools' input to "search for workflow imports")
        :param wait: boolean (Wait for workflow to complete before returning, could yield long response times)
        """
        email = get_email_from_request(request)
        # When accessing a missing entry in the params dict, the resulting KeyError
        # exception gets translated to a Bad Request error in the try block below.
        params = await request.json()

        ar_guid = generate_ar_guid()

        job_args = get_args_from_params(
            params,
            email=email,
            server_config=get_server_config(),
        )

        config = get_baseline_run_config(
            ar_guid=ar_guid,
            environment=job_args.cloud_environment,
            gcp_project_id=job_args.gcp_project,
            dataset=job_args.dataset,
            access_level=job_args.access_level,
            output_prefix=job_args.output,
        )

        timestamp = datetime.now().astimezone().isoformat()
        workflow_output_dir: str = get_workflow_output_dir(config, job_args)

        # Prepare the job's configuration and write it to a blob.

        if user_config := params.get('config'):  # Update with user-specified configs.
            update_dict(config, user_config)
        config_path = write_config(ar_guid, config, job_args.cloud_environment)

        user_name = email.split('@')[0]
        batch_name = f'{user_name} {job_args.repo}:{job_args.commit}/cromwell/{job_args.workflow}'

        # This metadata dictionary gets stored at the output_dir location.
        metadata = get_analysis_runner_metadata(
            ar_guid=ar_guid,
            name=batch_name,
            timestamp=timestamp,
            dataset=job_args.dataset,
            user=email,
            access_level=job_args.access_level,
            repo=job_args.repo,
            commit=job_args.commit,
            script=job_args.workflow,
            description=params['description'],
            output_prefix=workflow_output_dir,
            driver_image=job_args.image,
            config_path=config_path,
            cwd=job_args.cwd,
            mode='cromwell',
            # no support for other environments
            environment=job_args.cloud_environment,
        )

        hail_bucket = f'cpg-{job_args.dataset}-hail'
        backend = hb.ServiceBackend(
            billing_project=job_args.dataset,
            remote_tmpdir=remote_tmpdir(hail_bucket),
            token=job_args.hail_token,
        )

        attributes = {
            AR_GUID_NAME: ar_guid,
            'author': user_name,
            **job_args.get_batch_attributes(),
        }
        comments = job_args.get_batch_comments()

        batch = hb.Batch(
            backend=backend,
            name=batch_name,
            requester_pays_project=job_args.gcp_project,
            attributes=attributes,
        )

        job = batch.new_job(name='driver')
        job.command('\n'.join(f'echo {quote(comment)}' for comment in comments))
        job = prepare_git_job(
            job=job,
            repo_name=job_args.repo,
            commit=job_args.commit,
            print_all_statements=False,
            is_test=job_args.is_test(),
        )

        job.image(job_args.image)

        job.env('CPG_CONFIG_PATH', config_path)

        run_cromwell_workflow(
            job=job,
            dataset=job_args.dataset,
            access_level=job_args.access_level,
            workflow=job_args.workflow,
            cwd=job_args.cwd,
            libs=job_args.dependencies,
            labels=job_args.labels,
            output_prefix=job_args.output,
            input_dict=job_args.inputs,
            input_paths=job_args.input_jsons,
            project=job_args.gcp_project,
            ar_guid_override=ar_guid,
        )

        url = run_batch_job_and_print_url(
            batch,
            wait=params.get('wait', False),
            environment=job_args.cloud_environment,
        )

        # Publish the metadata to Pub/Sub.
        metadata['batch_url'] = url
        publisher.publish(PUBSUB_TOPIC, json.dumps(metadata).encode('utf-8')).result()

        return web.Response(text=f'{url}/jobs/1\n')

    @routes.get('/cromwell/{workflow_id}/metadata')
    async def get_cromwell_metadata(request: web.Request) -> web.Response:
        try:
            workflow_id = request.match_info['workflow_id']
            cromwell_metadata_url = (
                CROMWELL_URL
                + f'/api/workflows/v1/{workflow_id}/metadata?expandSubWorkflows=true'
            )

            token = get_cromwell_oauth_token()
            headers = {'Authorization': 'Bearer ' + str(token)}
            # longer timeout because metadata can take a while to fetch
            req = requests.get(cromwell_metadata_url, headers=headers, timeout=120)
            if not req.ok:
                raise web.HTTPInternalServerError(
                    reason=req.content.decode() or req.reason,
                )
            return web.json_response(req.json())
        except web.HTTPError:
            raise
        except Exception as e:  # noqa: BLE001
            raise web.HTTPInternalServerError(reason=str(e)) from e


def get_args_from_params(
    params: dict,
    email: str,
    server_config: dict,
) -> CromwellJobArgs:
    dataset = params['dataset']
    access_level = params['accessLevel']
    cloud_environment = 'gcp'

    input_jsons = params.get('input_json_paths') or []
    input_dict = params.get('inputs_dict')

    dataset_config = check_dataset_and_group(
        server_config=server_config,
        environment=cloud_environment,
        dataset=dataset,
        email=email,
    )
    environment_config = dataset_config.get(cloud_environment, {})
    hail_token = get_hail_token(dataset, dataset_config, access_level)

    repo = get_and_check_repository(params, dataset_config)
    if not repo:
        raise web.HTTPBadRequest(reason='Must supply a "repo"')
    commit = get_and_check_commit(params, repo)
    if not commit:
        raise web.HTTPBadRequest(reason='Must supply a "commit"')

    check_allowed_repos(dataset_config=dataset_config, repo=repo)
    output = validate_output_dir(params['output'])

    libs = params.get('dependencies')
    if not isinstance(libs, list):
        raise web.HTTPBadRequest(reason='Expected "dependencies" to be a list')

    wf = params['workflow']
    if not wf:
        raise web.HTTPBadRequest(reason='Invalid "workflow" parameter')

    return CromwellJobArgs(
        dataset=dataset,
        branch=params.get('branch'),
        inputs=input_dict,
        input_jsons=input_jsons,
        access_level=access_level,
        cloud_environment='gcp',
        output=output,
        description=params['description'],
        labels=params.get('labels'),
        repo=repo,
        commit=commit,
        cwd=params['cwd'],
        workflow=wf,
        dependencies=libs,
        gcp_project=environment_config.get('projectId'),
        hail_token=hail_token,
        # will default to the DRIVER_IMAGE
        image=get_and_check_image({}, is_test=access_level == 'test'),
    )


def get_workflow_output_dir(config: dict, job_args: CromwellJobArgs) -> str:
    bucket_path = config.get('storage', {}).get('default')
    if not bucket_path:
        if job_args.is_test():
            bucket_path = f'gs://cpg-{job_args.dataset}-test'
        else:
            bucket_path = f'gs://cpg-{job_args.dataset}-main'

    return os.path.join(bucket_path, job_args.output)
