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
    get_server_config,
    publisher,
    prepare_git_job,
    run_batch_job_and_print_url,
    CROMWELL_URL,
    PUBSUB_TOPIC,
    DRIVER_IMAGE,
    get_cromwell_key,
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
        try:
            server_config = get_server_config()
            output_dir = validate_output_dir(params['output'])
            dataset = params['dataset']
            check_dataset_and_group(server_config, dataset, email)
            repo = params['repo']
            check_allowed_repos(server_config, dataset, repo)
            access_level = params['accessLevel']

            ds_config = server_config[dataset]
            project = ds_config.get('projectId')
            hail_token = ds_config.get(f'{access_level}Token')
            service_account_json = get_cromwell_key(
                dataset=dataset, access_level=access_level
            )
            # use the email specified by the service_account_json again
            service_account_dict = json.loads(service_account_json)
            service_account_email = service_account_dict.get('client_email')
            if not service_account_email:
                raise web.HTTPServerError(
                    reason="The service_account didn't contain an entry for client_email"
                )

            if access_level == 'test':
                intermediate_dir = f'gs://cpg-{dataset}-test-tmp/cromwell'
            else:
                intermediate_dir = f'gs://cpg-{dataset}-main-tmp/cromwell'

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

            input_jsons = params.get('input_json_paths', [])
            input_dict = params.get('inputs_dict')

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
                print_all_statements=False,
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

            cromwell_post_url = CROMWELL_URL + '/api/workflows/v1'
            workflow_options = {
                'user_service_account_json': service_account_json,
                'google_compute_service_account': service_account_email,
                'google_project': project,
                'jes_gcs_root': intermediate_dir,
                'final_workflow_outputs_dir': output_dir,
            }

            if input_dict:
                tmp_input_json_path = '/tmp/inputs.json'
                job.command(f"echo '{json.dumps(input_dict)}' > {tmp_input_json_path}")
                input_jsons.append(tmp_input_json_path)

            inputs_cli = []
            for idx, value in enumerate(input_jsons):
                key = 'workflowInputs'
                if idx > 0:
                    key += f'_{idx + 1}'

                inputs_cli.append(f'-F "{key}=@{value}"')

            job.command(
                f"""
echo '{json.dumps(workflow_options)}' > workflow-options.json
access_token=$(gcloud auth print-identity-token --audiences=717631777761-ec4u8pffntsekut9kef58hts126v7usl.apps.googleusercontent.com)
wid=$(curl -X POST "{cromwell_post_url}" \\
    -H "Authorization: Bearer $access_token" \\
    -H "accept: application/json" \\
    -H "Content-Type: multipart/form-data" \\
    -F "workflowSource=@{wf}" \\
    {' '.join(inputs_cli)} \\
    -F "workflowOptions=@workflow-options.json;type=application/json" \\
    {f'-F "workflowDependencies=@{deps_path}"' if deps_path else ''})

echo "Submitted workflow with ID $wid"
"""
            )

            url = run_batch_job_and_print_url(batch, wait=params.get('wait', False))

            return web.Response(text=f'{url}\n')
        except KeyError as e:
            logging.error(e)
            raise web.HTTPBadRequest(reason='Missing request parameter: ' + e.args[0])
