# pylint: disable=unused-variable
"""
Exports 'add_cromwell_routes', to add the following route to a flask API:
    POST /cromwell: Posts a workflow to a cromwell_url
"""
import json
from datetime import datetime

import hailtop.batch as hb
import requests
from aiohttp import web
from cpg_utils.config import AR_GUID_NAME, update_dict
from cpg_utils.hail_batch import remote_tmpdir

# pylint: disable=wrong-import-order
from util import (
    DRIVER_IMAGE,
    PUBSUB_TOPIC,
    check_allowed_repos,
    check_dataset_and_group,
    generate_ar_guid,
    get_analysis_runner_metadata,
    get_baseline_run_config,
    get_email_from_request,
    get_server_config,
    publisher,
    run_batch_job_and_print_url,
    validate_output_dir,
    write_config,
)

from analysis_runner.constants import CROMWELL_URL
from analysis_runner.cromwell import get_cromwell_oauth_token, run_cromwell_workflow
from analysis_runner.git import prepare_git_job


def add_cromwell_routes(
    routes,
):
    """Add cromwell route(s) to 'routes' flask API"""

    @routes.post('/cromwell')
    async def cromwell(request):  # pylint: disable=too-many-locals
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
        dataset = params['dataset']
        access_level = params['accessLevel']
        cloud_environment = 'gcp'
        output_dir = validate_output_dir(params['output'])
        dataset_config = check_dataset_and_group(
            server_config=get_server_config(),
            environment=cloud_environment,
            dataset=dataset,
            email=email,
        )
        environment_config = dataset_config.get(cloud_environment)
        repo = params['repo']
        check_allowed_repos(dataset_config=dataset_config, repo=repo)
        labels = params.get('labels')

        project = environment_config.get('projectId')
        hail_token = environment_config.get(f'{access_level}Token')

        if not hail_token:
            raise web.HTTPBadRequest(
                reason=f"Invalid access level '{access_level}', couldn't find corresponding hail token"
            )

        commit = params['commit']
        if not commit or commit == 'HEAD':
            raise web.HTTPBadRequest(reason='Invalid commit parameter')

        libs = params.get('dependencies')
        if not isinstance(libs, list):
            raise web.HTTPBadRequest(reason='Expected "dependencies" to be a list')
        cwd = params['cwd']
        wf = params['workflow']
        if not wf:
            raise web.HTTPBadRequest(reason='Invalid script parameter')

        input_jsons = params.get('input_json_paths') or []
        input_dict = params.get('inputs_dict')

        if access_level == 'test':
            workflow_output_dir = f'gs://cpg-{dataset}-test/{output_dir}'
        else:
            workflow_output_dir = f'gs://cpg-{dataset}-main/{output_dir}'

        timestamp = datetime.now().astimezone().isoformat()

        # Prepare the job's configuration and write it to a blob.

        config = get_baseline_run_config(
            ar_guid=ar_guid,
            environment=cloud_environment,
            gcp_project_id=project,
            dataset=dataset,
            access_level=access_level,
            output_prefix=output_dir,
        )
        if user_config := params.get('config'):  # Update with user-specified configs.
            update_dict(config, user_config)
        config_path = write_config(ar_guid, config, cloud_environment)

        user_name = email.split('@')[0]
        batch_name = f'{user_name} {repo}:{commit}/cromwell/{wf}'

        # This metadata dictionary gets stored at the output_dir location.
        metadata = get_analysis_runner_metadata(
            ar_guid=ar_guid,
            name=batch_name,
            timestamp=timestamp,
            dataset=dataset,
            user=email,
            access_level=access_level,
            repo=repo,
            commit=commit,
            script=wf,
            description=params['description'],
            output_prefix=workflow_output_dir,
            driver_image=DRIVER_IMAGE,
            config_path=config_path,
            cwd=cwd,
            mode='cromwell',
            # no support for other environments
            environment=cloud_environment,
        )

        hail_bucket = f'cpg-{dataset}-hail'
        backend = hb.ServiceBackend(
            billing_project=dataset,
            remote_tmpdir=remote_tmpdir(hail_bucket),
            token=hail_token,
        )

        batch = hb.Batch(
            backend=backend,
            name=batch_name,
            requester_pays_project=project,
            attributes={AR_GUID_NAME: ar_guid},
        )

        job = batch.new_job(name='driver')
        job = prepare_git_job(
            job=job,
            repo_name=repo,
            commit=commit,
            print_all_statements=False,
            is_test=access_level == 'test',
        )

        job.image(DRIVER_IMAGE)

        job.env('CPG_CONFIG_PATH', config_path)

        run_cromwell_workflow(
            job=job,
            dataset=dataset,
            access_level=access_level,
            workflow=wf,
            cwd=cwd,
            libs=libs,
            labels=labels,
            output_prefix=output_dir,
            input_dict=input_dict,
            input_paths=input_jsons,
            project=project,
            ar_guid_override=ar_guid,
        )

        url = run_batch_job_and_print_url(
            batch, wait=params.get('wait', False), environment=cloud_environment
        )

        # Publish the metadata to Pub/Sub.
        metadata['batch_url'] = url
        publisher.publish(PUBSUB_TOPIC, json.dumps(metadata).encode('utf-8')).result()

        return web.Response(text=f'{url}\n')

    @routes.get('/cromwell/{workflow_id}/metadata')
    async def get_cromwell_metadata(request):
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
                    reason=req.content.decode() or req.reason
                )
            return web.json_response(req.json())
        except web.HTTPError:
            raise
        except Exception as e:
            raise web.HTTPInternalServerError(reason=str(e)) from e
