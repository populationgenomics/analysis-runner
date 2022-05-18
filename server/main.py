"""The analysis-runner server, running Hail Batch pipelines on users' behalf."""
# pylint: disable=wrong-import-order
import datetime
import json
import logging
from shlex import quote
import hailtop.batch as hb
from aiohttp import web
from analysis_runner.git import prepare_git_job
from cromwell import add_cromwell_routes
from util import (
    DRIVER_IMAGE,
    IMAGE_REGISTRY_PREFIX,
    PUBSUB_TOPIC,
    REFERENCE_PREFIX,
    _get_hail_version,
    check_allowed_repos,
    check_dataset_and_group,
    get_analysis_runner_metadata,
    get_email_from_request,
    get_server_config,
    publisher,
    run_batch_job_and_print_url,
    validate_image,
    validate_output_dir,
    write_config,
    write_metadata_to_bucket,
)
from cpg_utils.hail_batch import remote_tmpdir

logging.basicConfig(level=logging.INFO)
# do it like this so it's easy to disable
USE_GCP_LOGGING = True
if USE_GCP_LOGGING:
    import google.cloud.logging  # pylint: disable=import-error,no-name-in-module,c-extension-no-member

    client = google.cloud.logging.Client()
    client.get_default_handler()
    client.setup_logging()

routes = web.RouteTableDef()


# pylint: disable=too-many-statements
@routes.post('/')
async def index(request):
    """Main entry point, responds to the web root."""

    email = get_email_from_request(request)
    # When accessing a missing entry in the params dict, the resulting KeyError
    # exception gets translated to a Bad Request error in the try block below.
    params = await request.json()

    server_config = get_server_config()
    output_prefix = validate_output_dir(params['output'])
    dataset = params['dataset']
    check_dataset_and_group(server_config, dataset, email)
    repo = params['repo']
    check_allowed_repos(server_config, dataset, repo)

    image = params.get('image') or DRIVER_IMAGE
    cpu = params.get('cpu', 1)
    memory = params.get('memory', '1G')
    environment_variables = params.get('environmentVariables')

    access_level = params['accessLevel']
    is_test = access_level == 'test'
    hail_token = server_config[dataset].get(f'{access_level}Token')
    if not hail_token:
        raise web.HTTPBadRequest(reason=f'Invalid access level "{access_level}"')

    if not validate_image(image, is_test):
        raise web.HTTPBadRequest(reason=f'Invalid image "{image}"')

    hail_bucket = f'cpg-{dataset}-hail'
    backend = hb.ServiceBackend(
        billing_project=dataset,
        remote_tmpdir=remote_tmpdir(hail_bucket),
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
    metadata = get_analysis_runner_metadata(
        timestamp=timestamp,
        dataset=dataset,
        user=email,
        access_level=access_level,
        repo=repo,
        commit=commit,
        script=' '.join(script),
        description=params['description'],
        output_prefix=output_prefix,
        hailVersion=hail_version,
        driver_image=image,
        cwd=cwd,
    )

    user_name = email.split('@')[0]
    batch_name = f'{user_name} {repo}:{commit}/{" ".join(script)}'

    dataset_gcp_project = server_config[dataset]['projectId']
    batch = hb.Batch(
        backend=backend, name=batch_name, requester_pays_project=dataset_gcp_project
    )

    job = batch.new_job(name='driver')
    job = prepare_git_job(job=job, repo_name=repo, commit=commit, is_test=is_test)
    write_metadata_to_bucket(
        job,
        access_level=access_level,
        dataset=dataset,
        output_prefix=output_prefix,
        metadata_str=json.dumps(metadata),
    )
    job.image(image)
    if cpu:
        job.cpu(cpu)
    if memory:
        job.memory(memory)

    # Prepare the job's configuration, which will be written to a blob.
    config = {
        'hail': {
            'billing_project': dataset,
            'bucket': hail_bucket,
        },
        'workflow': {
            'access_level': access_level,
            'dataset': dataset,
            'dataset_gcp_project': dataset_gcp_project,
            'driver_image': DRIVER_IMAGE,
            'image_registry_prefix': IMAGE_REGISTRY_PREFIX,
            'reference_prefix': REFERENCE_PREFIX,
            'output_prefix': output_prefix,
        },
    }

    # NOTE: Prefer using config variables instead of environment variables.
    # In case you need to add an environment variable here, make sure to update the
    # cpg_utils.hail_batch.copy_common_env function!
    job.env('CPG_CONFIG_PATH', write_config(config))

    if environment_variables:
        if not isinstance(environment_variables, dict):
            raise ValueError('Expected environment_variables to be dictionary')

        invalid_env_vars = [
            f'{k}={v}'
            for k, v in environment_variables.items()
            if not isinstance(v, str)
        ]

        if len(invalid_env_vars) > 0:
            raise ValueError(
                'Some environment_variables values were not strings, got '
                + ', '.join(invalid_env_vars)
            )

        for k, v in environment_variables.items():
            job.env(k, v)

    if cwd:
        job.command(f'cd {quote(cwd)}')

    job.command(f'which {quote(script[0])} || chmod +x {quote(script[0])}')

    # Finally, run the script.
    escaped_script = ' '.join(quote(s) for s in script if s)
    job.command(escaped_script)

    url = run_batch_job_and_print_url(batch, wait=params.get('wait', False))

    # Publish the metadata to Pub/Sub.
    metadata['batch_url'] = url
    publisher.publish(PUBSUB_TOPIC, json.dumps(metadata).encode('utf-8')).result()

    return web.Response(text=f'{url}\n')


add_cromwell_routes(routes)


def prepare_exception_json_response(status_code: int, message: str) -> web.Response:
    """Prepare web.Response for"""
    return web.Response(
        status=status_code,
        body=json.dumps({'message': message, 'success': False}).encode('utf-8'),
        content_type='application/json',
    )


def prepare_response_from_exception(ex: Exception):
    """Prepare json_response from exception"""
    logging.error(f'Request failed with exception: {repr(ex)}')

    if isinstance(ex, web.HTTPException):
        return prepare_exception_json_response(
            status_code=ex.status_code, message=ex.reason
        )
    if isinstance(ex, KeyError):
        keys = ', '.join(ex.args)
        return prepare_exception_json_response(
            400, f'Missing request parameter: {keys}'
        )
    if isinstance(ex, ValueError):
        return prepare_exception_json_response(400, ', '.join(ex.args))

    if hasattr(ex, 'message'):
        m = ex.message
    else:
        m = str(ex)
    return prepare_exception_json_response(500, message=m)


async def error_middleware(_, handler):
    """
    Constructs middleware handler
    First argument is app, but unused in this context
    """

    async def middleware_handler(request):
        """
        Run handler and catch exceptions and response errors
        """
        try:
            response = await handler(request)
            if isinstance(response, web.HTTPException):
                return prepare_response_from_exception(response)
            return response
        # pylint: disable=broad-except
        except Exception as e:
            return prepare_response_from_exception(e)

    return middleware_handler


async def init_func():
    """Initializes the app."""
    app = web.Application(middlewares=[error_middleware])
    app.add_routes(routes)
    return app


if __name__ == '__main__':
    web.run_app(init_func())
