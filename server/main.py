"""The analysis-runner server, running Hail Batch pipelines on users' behalf."""

import datetime
import json
import logging
from shlex import quote
from aiohttp import web

import hailtop.batch as hb

from cromwell import add_cromwell_routes

from util import (
    get_analysis_runner_metadata,
    get_email_from_request,
    _get_hail_version,
    validate_output_dir,
    check_dataset_and_group,
    check_allowed_repos,
    publisher,
    prepare_git_job,
    run_batch_job_and_print_url,
    get_server_config,
    DRIVER_IMAGE,
    PUBSUB_TOPIC,
)

routes = web.RouteTableDef()


# pylint: disable=too-many-statements
@routes.post('/')
async def index(request):
    """Main entry point, responds to the web root."""

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
        hail_token = server_config[dataset].get(f'{access_level}Token')
        if not hail_token:
            raise web.HTTPBadRequest(reason=f'Invalid access level "{access_level}"')

        hail_bucket = f'cpg-{dataset}-hail'
        backend = hb.ServiceBackend(
            billing_project=dataset,
            bucket=hail_bucket,
            token=hail_token,
        )

        commit = params['commit']
        if not commit or commit == 'HEAD':
            raise web.HTTPBadRequest(reason='Invalid commit parameter')

        cwd = params['cwd']
        script = params['script']
        if not script:
            raise web.HTTPBadRequest(reason='Invalid script parameter')

        if not isinstance(script, list):
            raise web.HTTPBadRequest(reason='Script parameter expects an array')

        # This metadata dictionary gets stored in the metadata bucket, at the output_dir location.
        hail_version = await _get_hail_version()
        timestamp = datetime.datetime.now().astimezone().isoformat()
        metadata = json.dumps(
            get_analysis_runner_metadata(
                timestamp=timestamp,
                dataset=dataset,
                user=email,
                access_level=access_level,
                repo=repo,
                commit=commit,
                script=' '.join(script),
                description=params['description'],
                output=output_dir,
                hailVersion=hail_version,
                driver_image=DRIVER_IMAGE,
                cwd=cwd,
            )
        )

        # Publish the metadata to Pub/Sub. Wait for the result before running the batch.
        publisher.publish(PUBSUB_TOPIC, metadata.encode('utf-8')).result()

        user_name = email.split('@')[0]
        batch_name = f'{user_name} {repo}:{commit}/{" ".join(script)}'

        batch = hb.Batch(backend=backend, name=batch_name)

        job = batch.new_job(name='driver')
        job = prepare_git_job(
            job=job,
            dataset=dataset,
            access_level=access_level,
            output_dir=output_dir,
            repo=repo,
            commit=commit,
            metadata_str=metadata,
        )
        job.env('HAIL_BUCKET', hail_bucket)
        job.env('HAIL_BILLING_PROJECT', dataset)
        job.env('DATASET_GCP_PROJECT', server_config[dataset]['projectId'])
        job.env('OUTPUT', output_dir)
        if cwd:
            job.command(f'cd {quote(cwd)}')

        job.command(f'which {quote(script[0])} || chmod +x {quote(script[0])}')

        # Finally, run the script.
        escaped_script = ' '.join(quote(s) for s in script if s)
        job.command(escaped_script)

        url = run_batch_job_and_print_url(batch, wait=params.get('wait', False))

        return web.Response(text=f'{url}\n')
    except KeyError as e:
        logging.error(e)
        raise web.HTTPBadRequest(reason=f'Missing request parameter: {e.args[0]}')


add_cromwell_routes(routes)


async def init_func():
    """Initializes the app."""
    app = web.Application()
    app.add_routes(routes)
    return app


if __name__ == '__main__':
    web.run_app(init_func())
