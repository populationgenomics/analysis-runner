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

from analysis_runner.constants import CROMWELL_URL
from analysis_runner.cromwell import get_cromwell_oauth_token, run_cromwell_workflow
from analysis_runner.git import prepare_git_job
from analysis_runner.util import get_server_config
from server.util import write_metadata_to_bucket
from util import (
    get_analysis_runner_metadata,
    get_email_from_request,
    validate_output_dir,
    check_dataset_and_group,
    check_allowed_repos,
    publisher,
    run_batch_job_and_print_url,
    PUBSUB_TOPIC,
    DRIVER_IMAGE,
)


def add_cromwell_routes(
    routes,
):
    """Add cromwell route(s) to 'routes' flask API"""

    @routes.post('/cromwell')
    async def cromwell(request):  # pylint: disable=too-many-locals
        """
        Checks out a repo, and POSTs the designated workflow to the cromwell server
        ---
        post:
          operationId: createCromwellRequest
          requestBody:
            required: true
            content:
              application/json:
                schema:
                  type: object
                  properties:
                    output:
                      type: string
                    dataset:
                      type: string
                    accessLevel:
                      type: string
                    repo:
                      type: string
                    commit:
                      type: string
                    cwd:
                      type: string
                      description: to set the working directory, relative to the repo root
                    description:
                      type: string
                      description: Description of the workflow to run
                    workflow:
                      type: string
                      description: the relative path of the workflow (from the cwd)
                    input_json_paths:
                      type: List[string]
                      description: the relative path to an inputs.json (from the cwd). Currently only supports one inputs.json
                    dependencies:
                      type: array
                      items: string
                      description: 'An array of directories (/ files) to zip for the '-p / --tools' input to "search for workflow imports"
                    wait:
                      type: boolean
                      description: 'Wait for workflow to complete before returning, could yield long response times'


          responses:
            '200':
              content:
                text/plain:
                  schema:
                    type: string
                    example: 'batch.hail.populationgenomics.org.au/batches/{batch}'
              description: URL of submitted hail batch workflow
        """
        email = get_email_from_request(request)
        # When accessing a missing entry in the params dict, the resulting KeyError
        # exception gets translated to a Bad Request error in the try block below.
        params = await request.json()

        server_config = get_server_config()
        output_dir = validate_output_dir(params['output'])
        dataset = params['dataset']
        check_dataset_and_group(server_config, dataset, email)
        repo = params['repo']
        check_allowed_repos(server_config, dataset, repo)
        access_level = params['accessLevel']
        labels = params.get('labels')

        ds_config = server_config[dataset]
        project = ds_config.get('projectId')
        hail_token = ds_config.get(f'{access_level}Token')
        # use the email specified by the service_account_json again

        hail_bucket = f'cpg-{dataset}-hail'
        backend = hb.ServiceBackend(
            billing_project=dataset,
            bucket=hail_bucket,
            token=hail_token,
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

        # This metadata dictionary gets stored at the output_dir location.
        timestamp = datetime.now().astimezone().isoformat()
        metadata = get_analysis_runner_metadata(
                timestamp=timestamp,
                dataset=dataset,
                user=email,
                access_level=access_level,
                repo=repo,
                commit=commit,
                script=wf,
                description=params['description'],
                output_suffix=workflow_output_dir,
                driver_image=DRIVER_IMAGE,
                cwd=cwd,
                mode='cromwell',
                server_config=server_config,

        )

        # Publish the metadata to Pub/Sub. Wait for the result before running the batch.
        publisher.publish(PUBSUB_TOPIC, metadata.encode('utf-8')).result()

        user_name = email.split('@')[0]
        batch_name = f'{user_name} {repo}:{commit}/cromwell/{wf}'

        batch = hb.Batch(
            backend=backend, name=batch_name, requester_pays_project=project
        )

        job = batch.new_job(name='driver')
        job = prepare_git_job(
            job=job,
            repo=repo,
            commit=commit,
            print_all_statements=False,
            is_test=access_level == 'test'
        )

        write_metadata_to_bucket(
            job,
            access_level=access_level,
            dataset=dataset,
            output_suffix=output_dir,
            metadata_str=json.dumps(metadata),
        )
        job.image(DRIVER_IMAGE)

        job.env('DRIVER_IMAGE', DRIVER_IMAGE)
        job.env('DATASET', dataset)
        job.env('ACCESS_LEVEL', access_level)
        job.env('OUTPUT', output_dir)

        run_cromwell_workflow(
            job=job,
            dataset=dataset,
            access_level=access_level,
            workflow=wf,
            cwd=cwd,
            libs=libs,
            labels=labels,
            output_suffix=output_dir,
            input_dict=input_dict,
            input_paths=input_jsons,
        )

        url = run_batch_job_and_print_url(batch, wait=params.get('wait', False))

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
            req = requests.get(cromwell_metadata_url, headers=headers)
            if not req.ok:
                raise web.HTTPInternalServerError(
                    reason=req.content.decode() or req.reason
                )
            return web.json_response(req.json())
        except web.HTTPError:
            raise
        except Exception as e:
            raise web.HTTPInternalServerError(reason=str(e)) from e
