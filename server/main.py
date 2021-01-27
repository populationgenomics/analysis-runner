from aiohttp import web
from google.auth import jwt
import hailtop.batch as hb 
from hailtop.config import get_deploy_config

GITHUB_ORG = 'populationgenomics'

ALLOWED_REPOS = {
    'tob-wgs',
}

BATCH_DRIVER_IMAGE = 'australia-southeast1-docker.pkg.dev/analysis-runner/images/batch-driver:cff2885ebab7'

routes = web.RouteTableDef()

def _email_from_id_token(auth_header: str) -> str:
    '''Decodes the ID token (JWT) to get the email address of the caller.

    See https://developers.google.com/identity/sign-in/web/backend-auth?authuser=0#verify-the-integrity-of-the-id-token
    for details.

    This function assumes that the token has been verified beforehand.'''

    id_token = auth_header[7:]  # Strip the 'bearer' / 'Bearer' prefix.
    id_info = jwt.decode(id_token, verify=False)
    return id_info['email']

def _check_auth(email: str, project: str) -> None:
    '''If the given user doesn't have access to the project as defined by the
    corresponding permissions group, this function raises an HTTPUnauthorized
    exception.'''

    raise web.HTTPUnauthorized()

@routes.post('/')
async def index(request):
    auth_header = request.headers.get('Authorization')
    if auth_header is None:
        raise web.HTTPUnauthorized()

    # When accessing a missing entry in the params dict, the resulting KeyError
    # exception gets translated to a Bad Request error in the try block below.
    params = await request.json()
    try:
        project = params['project']

        email = _email_from_id_token(auth_header)
        _check_auth(email, project)

    # prevent command injection
    # ' '.join(["'{}'".format(x.replace("'", "'\\''")) for x in a.split(' ') if len(x)])

        repo = params['repo']
        if repo not in ALLOWED_REPOS:
            raise web.HTTPForbidden()

        backend = hb.ServiceBackend(
            billing_project=params['hailBillingProject'],
            bucket=params['hailBucket'],
            token=hail_token)

        now = datetime.now()
        batch_name = (f'analysis-runner {repo}:{commit}/{path} '
                      f'{now.strftime("%Y-%m-%d %H-%M-%S")}')

        batch = hb.Batch(backend=backend, name=batch_name)

        # TODO: add Airtable metadata submission, supporting descriptions and
        # output directory. Set up environment variable for script to
        # write output to.
        job = batch.new_job(name='driver')
        job.image(BATCH_DRIVER_IMAGE)
        # TODO: Add GitHub authorization for non-public repos.
        job.command(f'git clone https://github.com/{GITHUB_ORG}/{repo}')
        job.command(f'cd {repo}')
        job.command('git checkout main')
        # Check whether the given commit is in the main branch.
        commit = params['commit']
        job.command(f'git merge-base --is-ancestor {commit} HEAD')
        job.command(f'git checkout {commit}')
        # TODO: check that script file exists: test -f $FILE
        # TODO: escape parameters (also for commit)
        job.command(f'python3 {params["scriptPath"]}')

        bc_batch = batch.run(wait=False)
        deploy_config = get_deploy_config()
        url = deploy_config.url('batch', f'/batches/{bc_batch.id}')

        return web.Response(text=f'{url}\n')
    except:
        raise web.HTTPBadRequest()


async def init_func():
    app = web.Application()
    app.add_routes(routes)
    return app
