"""The analysis-runner server, running Hail Batch pipelines on users' behalf."""

# ruff: noqa: E402
import json
import logging
import traceback

import nest_asyncio
from aiohttp import web
from ar import add_analysis_runner_routes
from config import add_config_routes
from cromwell import add_cromwell_routes

# Patching asyncio *before* importing the Hail Batch module is necessary to avoid a
# "Cannot enter into task" error.
nest_asyncio.apply()


logging.basicConfig(level=logging.INFO)
# do it like this so it's easy to disable
USE_GCP_LOGGING = True
if USE_GCP_LOGGING:
    import google.cloud.logging

    client = google.cloud.logging.Client()
    client.get_default_handler()
    client.setup_logging()


def prepare_exception_json_response(
    status_code: int,
    message: str,
    tb: str,
) -> web.Response:
    """Prepare web.Response for"""
    return web.Response(
        status=status_code,
        body=json.dumps({'message': message, 'success': False, 'traceback': tb}).encode(
            'utf-8',
        ),
        content_type='application/json',
    )


def prepare_response_from_exception(ex: Exception):
    """Prepare json_response from exception"""
    tb = ''.join(traceback.format_exception(type(ex), ex, ex.__traceback__))

    logging.error(f'Request failed with exception:\n{tb}')

    if isinstance(ex, web.HTTPException):
        return prepare_exception_json_response(
            status_code=ex.status_code,
            message=ex.reason,
            tb=tb,
        )
    if isinstance(ex, KeyError):
        keys = ', '.join(ex.args)
        return prepare_exception_json_response(
            400,
            message=f'Missing request parameter: {keys}',
            tb=tb,
        )
    if isinstance(ex, ValueError):
        return prepare_exception_json_response(400, ', '.join(ex.args), tb=tb)

    m = ex.message if hasattr(ex, 'message') else str(ex)
    return prepare_exception_json_response(500, message=m, tb=tb)


async def error_middleware(_, handler):  # noqa: ANN001
    """
    Constructs middleware handler
    First argument is app, but unused in this context
    """

    async def middleware_handler(request: web.Request) -> web.Response:
        """
        Run handler and catch exceptions and response errors
        """
        try:
            response = await handler(request)
            if isinstance(response, web.HTTPException):
                return prepare_response_from_exception(response)
            return response

        except Exception as e:  # noqa: BLE001
            return prepare_response_from_exception(e)

    return middleware_handler


async def init_func():
    """Initializes the app."""
    app = web.Application(middlewares=[error_middleware])
    routes = web.RouteTableDef()

    add_analysis_runner_routes(routes)
    add_cromwell_routes(routes)
    add_config_routes(routes)
    app.add_routes(routes)

    return app


if __name__ == '__main__':
    web.run_app(init_func())
