# pylint: disable=unused-variable
"""
Exports 'add_cromwell_routes', to add the following route to a flask API:
    POST /cromwell: Posts a workflow to a cromwell_url
"""
import json
import logging
from datetime import datetime
from shlex import quote
from aiohttp import web

import hailtop.batch as hb

from util import (
    get_analysis_runner_metadata,
    get_email_from_request,
    validate_output_dir,
    check_dataset_and_group,
    check_allowed_repos,
    server_config,
    publisher,
    prepare_git_job,
    run_batch_job_and_print_url,
    cromwell_url,
    PUBSUB_TOPIC,
    DRIVER_IMAGE,
)


def add_cromwell_routes(
    routes,
):
    """Add cromwell route(s) to 'routes' flask API"""

    @routes.post('/cromwell')
    async def cromwell(request):
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
                    inputs:
                      type: string
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
                    example: 'batch.hail.populationgenomics.org.au/batches/{batch}/jobs/'
              description: URL of submitted hail batch workflow
        """
        email = get_email_from_request(request)
        # When accessing a missing entry in the params dict, the resulting KeyError
        # exception gets translated to a Bad Request error in the try block below.
        params = await request.json()
        try:
            output_dir = validate_output_dir(params['output'])
            dataset = params['dataset']
            check_dataset_and_group(dataset, email)
            repo = params['repo']
            check_allowed_repos(dataset, repo)
            access_level = params['accessLevel']

            ds_config = server_config[dataset]
            hail_token = ds_config.get(f'{access_level}Token')
            project = ds_config.get('projectId')
            service_account_json = ds_config.get(f'{access_level}Key')
            intermediate_dir = f'gs://cpg-{dataset}-temporary/cromwell'

            if not service_account_json or not intermediate_dir:
                raise web.HTTPBadRequest(
                    reason=f'Invalid access level "{access_level}"'
                )

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

            inputs_json = params['inputs']

            # This metadata dictionary gets stored at the output_dir location.
            timestamp = datetime.now().astimezone().isoformat()
            metadata = json.dumps(
                get_analysis_runner_metadata(
                    timestamp=timestamp,
                    dataset=dataset,
                    user=email,
                    access_level=access_level,
                    repo=repo,
                    commit=commit,
                    script=wf,
                    description=params['description'],
                    output=output_dir,
                    driver_image=DRIVER_IMAGE,
                    cwd=cwd,
                    mode='cromwell',
                )
            )

            # Publish the metadata to Pub/Sub. Wait for the result before running the batch.
            publisher.publish(PUBSUB_TOPIC, metadata.encode('utf-8')).result()

            user_name = email.split('@')[0]
            batch_name = f'{user_name} {repo}:{commit}/cromwell/{wf}'

            batch = hb.Batch(backend=backend, name=batch_name)

            job = batch.new_job(name='driver')
            job = prepare_git_job(
                job=job,
                dataset=dataset,
                access_level=access_level,
                repo=repo,
                commit=commit,
                metadata_str=metadata,
                output_dir=output_dir,
            )
            job.env('OUTPUT', output_dir)
            if cwd:
                job.command(f'cd {quote(cwd)}')

            deps_path = None
            if libs:
                deps_path = 'tools.zip'
                job.command(
                    'zip -r tools.zip ' + ' '.join(quote(s + '/') for s in libs)
                )

            cromwell_post_url = cromwell_url + 'api/workflows/v1/'
            workflow_options = {
                'user_service_account_json': service_account_json,
                'google_project': project,
                'jes_gcs_root': intermediate_dir,
                'final_workflow_outputs_dir': output_dir,
            }

            job.command(
                f"""
cat '{json.dumps(workflow_options)}' > workflow_options.json
curl -X POST "{cromwell_post_url}" \\
    -H "accept: application/json" \\
    -H "Content-Type: multipart/form-data" \\
    -F "workflowSource=@{wf}" \\
    {f'-F "workflowInputs=@{inputs_json}"' if inputs_json else ''} \\
    -F "workflowOptions=@workflow-options.json;type=application/json" \\
    {f'-F "workflowOptions=@{deps_path}"' if deps_path else ''} \\
"""
            )

            url = run_batch_job_and_print_url(batch, wait=params.get('wait', False))

            return web.Response(text=f'{url}\n')
        except KeyError as e:
            logging.error(e)
            raise web.HTTPBadRequest(reason='Missing request parameter: ' + e.args[0])
