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
    server_config,
    publisher,
    prepare_git_job,
    run_batch_job_and_print_url,
    cromwell_url,
    PUBSUB_TOPIC,
    DRIVER_IMAGE,
)


def add_cromwel_routes(
    routes,
):
    def make_cromwell_url(suffix):
        return cromwell_url + suffix

    @routes.post('/cromwell')
    async def cromwell(request):
        email = get_email_from_request(request)
        # When accessing a missing entry in the params dict, the resulting KeyError
        # exception gets translated to a Bad Request error in the try block below.
        params = await request.json()
        try:
            output_dir = validate_output_dir(params['output'])

            dataset = params['dataset']
            check_dataset_and_group(dataset, email)

            repo = params['repo']
            allowed_repos = server_config[dataset]['allowedRepos']
            if repo not in allowed_repos:
                raise web.HTTPForbidden(
                    reason=(
                        f'Repository "{repo}" is not one of the allowed repositories: '
                        f'{", ".join(allowed_repos)}'
                    )
                )

            access_level = params['accessLevel']
            ds_config = server_config[dataset]
            hail_token = ds_config.get(f'{access_level}Token')
            project = ds_config.get(f'{access_level}Project')
            service_account = ds_config.get(f'{access_level}ServiceAccount')
            intermediate_dir = ds_config.get(f'{access_level}IntermediateDir')
            if not service_account or not intermediate_dir:
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

            libs = params.get('deps')
            if libs:
                libs.split(',')
            cwd = params['cwd']
            wf = params['workflow']
            if not wf:
                raise web.HTTPBadRequest(reason='Invalid script parameter')

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

            cromwell_post_url = make_cromwell_url('api/workflows/v1/')
            workflow_options = {
                'user_service_account_json': service_account,
                'google_project': project,
                'jes_gcs_root': intermediate_dir,
                'final_workflow_outputs_dir': output_dir,
            }

            workflow_inputs = None

            job.command(
                f"""
cat '{json.dumps(workflow_options)}' > workflow_options.json
curl -X POST "{cromwell_post_url}" \\
    -H "accept: application/json" \\
    -H "Content-Type: multipart/form-data" \\
    -F "workflowSource=@{wf}" \\
    {f'-F "workflowInputs=@{workflow_inputs}"' if workflow_inputs else ''} \\
    -F "workflowOptions=@workflow-options.json;type=application/json" \\
    {f'-F "workflowOptions=@{deps_path}"' if deps_path else ''} \\
"""
            )

            url = run_batch_job_and_print_url(batch, wait=params.get('wait', False))

            return web.Response(text=f'{url}\n')
        except KeyError as e:
            logging.error(e)
            raise web.HTTPBadRequest(reason='Missing request parameter: ' + e.args[0])

    @routes.get('/cromwell/engine/version')
    def get_engine_version(request):
        # proxy request to cromwell path
        pass

    @routes.post('/cromwell/{workflow_id}/abort')
    def abort_cromwell_workflow(request):
        wid = get_workflow_id(request)

    @routes.get('/cromwell/{workflow_id}/status')
    def get_cromwell_status(request):
        wid = get_workflow_id(request)

    @routes.get('/cromwell/{workflow_id}/outputs')
    def get_cromwell_outputs(request):
        wid = get_workflow_id(request)

    @routes.get('/cromwell/{workflow_id}/metadata')
    def get_cromwell_metadata(request):
        wid = get_workflow_id(request)

    def get_workflow_id(request):
        wid = request.match_info.get('workflow_id', None)
        if not wid:
            raise web.HTTPBadRequest(reason='Missing url parameter: workflow_id')
        return wid
