# ruff: noqa: E402
import dataclasses
import json

from aiohttp import web
from util import (
    check_dataset_and_group,
    get_and_check_cloud_environment,
    get_and_check_image,
    get_baseline_run_config,
    get_email_from_request,
    get_server_config,
    validate_output_dir,
)

from cpg_utils.config import update_dict


@dataclasses.dataclass
class AnalysisRunnerConfigArgs:
    dataset: str
    output_prefix: str
    access_level: str
    config: dict
    cloud_environment: str
    image: str
    gcp_project: str

    def is_test(self) -> bool:
        return self.access_level == 'test'


def add_config_routes(routes: web.RouteTableDef):

    @routes.post('/config')
    async def config(request: web.Request) -> web.Response:
        """
        Generate CPG config, as JSON response
        """
        email = get_email_from_request(request)
        # When accessing a missing entry in the params dict, the resulting KeyError
        # exception gets translated to a Bad Request error in the try block below.
        params = await request.json()

        args = get_args_from_params(
            params,
            email=email,
            server_config=get_server_config(),
        )

        # Prepare the job's configuration to return
        run_config = get_baseline_run_config(
            ar_guid='<generated-at-runtime>',
            environment=args.cloud_environment,
            gcp_project_id=args.gcp_project,
            dataset=args.dataset,
            access_level=args.access_level,
            output_prefix=args.output_prefix,
            driver=args.image,
        )
        if user_config := params.get('config'):  # Update with user-specified configs.
            update_dict(run_config, user_config)

        return web.Response(
            status=200,
            body=json.dumps(run_config).encode('utf-8'),
            content_type='application/json',
        )


def get_args_from_params(
    params: dict,
    email: str,
    server_config: dict,
) -> AnalysisRunnerConfigArgs:

    dataset = params['dataset']
    output_prefix = validate_output_dir(params['output'])
    access_level = params['accessLevel']
    is_test = access_level == 'test'

    cloud_environment = get_and_check_cloud_environment(params)

    dataset_config = check_dataset_and_group(
        server_config=server_config,
        environment=cloud_environment,
        dataset=dataset,
        email=email,
    )
    environment_config = dataset_config.get(cloud_environment, {})

    image = get_and_check_image(params, is_test=is_test)

    return AnalysisRunnerConfigArgs(
        dataset=dataset,
        output_prefix=output_prefix,
        access_level=access_level,
        config=params.get('config'),
        cloud_environment=cloud_environment,
        image=image,
        gcp_project=environment_config.get('projectId'),
    )
