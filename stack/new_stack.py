"""Create GCP project + stack file for Pulumi"""
# pylint: disable=unreachable
import json
import logging
import os
import re
import subprocess
import requests

import yaml
import click

TRUTHY_VALUES = ('y', '1', 't')
DATASET_REGEX = r'^[a-z][a-z0-9-]{1,15}[a-z]$'

ORGANIZATION_ID = '648561325637'
BILLING_ACCOUNT_ID = '01D012-20A6A2-CBD343'
BILLING_PROJECT_ID = 'billing-admin-290403'
HAIL_PROJECT = 'hail-295901'


RE_CLIENT_EMAIL_MATCHER = re.compile(
    f'[A-z0-9-]+@{HAIL_PROJECT}.iam.gserviceaccount.com'
)

GET_HAIL_TOKENS = (
    lambda project: f"""
for access_level in test standard full; do kubectl get secret {project}-$access_level-gsa-key -o json | jq '.data | map_values(@base64d)'; done
"""
)


@click.command()
@click.option('--dataset')
@click.option('--gcp-project', required=False, help='If different to the dataset name')
@click.option('--create-release-buckets', required=False, is_flag=True)
@click.option('--create-gcp-project', required=False, is_flag=True)
@click.option('--setup-gcp-billing', required=False, is_flag=True)
@click.option('--create-hail-service-accounts', required=False, is_flag=True)
@click.option('--prepare-pulumi-stack', required=False, is_flag=True)
@click.option('--add-to-seqr-stack', required=False, is_flag=True)
@click.option('--release-stack', required=False, is_flag=True)
@click.option('--generate-service-account-key', required=False, is_flag=True)
def main(
    dataset: str,
    gcp_project: str = None,
    create_release_buckets=False,
    create_gcp_project=False,
    setup_gcp_billing=False,
    create_hail_service_accounts=False,
    prepare_pulumi_stack=False,
    add_to_seqr_stack=False,
    release_stack=False,
    generate_service_account_key=False,
):
    """Function that coordinates creating a project"""
    dataset = dataset.lower()
    _gcp_project = (gcp_project or dataset).lower()

    if len(dataset) > 17:
        raise ValueError(
            f'The dataset length must be less than (or equal to) 17 characters (got {len(dataset)})'
        )

    match = re.fullmatch(DATASET_REGEX, dataset)
    if not match:
        raise ValueError(f'Expected dataset {dataset} to match {DATASET_REGEX}.')

    logging.info(f'Creating dataset "{dataset}" with GCP id {_gcp_project}.')

    if create_hail_service_accounts:
        create_hail_accounts(dataset)

    if create_gcp_project:
        create_project(project_id=_gcp_project)
        assign_billing_account(_gcp_project)

    if setup_gcp_billing:
        create_budget(_gcp_project)

    pulumi_config_fn = f'Pulumi.{dataset}.yaml'
    if prepare_pulumi_stack:
        generate_pulumi_stack_file(
            pulumi_config_fn=pulumi_config_fn,
            gcp_project=_gcp_project,
            add_to_seqr_stack=add_to_seqr_stack,
            dataset=dataset,
            create_release_buckets=create_release_buckets,
        )

    if not os.path.exists(pulumi_config_fn):
        raise ValueError(f'Expected to find {pulumi_config_fn}, but it did not exist')

    if release_stack:
        env = {**os.environ, 'PULUMI_CONFIG_PASSPHRASE': ''}
        subprocess.check_output(
            ['pulumi', 'stack', 'select', '--create', dataset], env=env
        )
        rc = subprocess.call(['pulumi', 'up', '-y'], env=env)
        if rc != 0:
            raise ValueError(f'The stack {dataset} did not deploy correctly')

    if generate_service_account_key:
        generate_upload_account_json(dataset=dataset, gcp_project=_gcp_project)


def create_project(project_id, organisation_id=ORGANIZATION_ID):
    """Call subprocess.check_output to create project under an organisation"""
    command = [
        'gcloud',
        'projects',
        'create',
        project_id,
        '--organization',
        organisation_id,
    ]
    subprocess.check_output(command)


def assign_billing_account(project_id, billing_account_id=BILLING_ACCOUNT_ID):
    """
    Assign a billing account to a GCP project
    """
    command = [
        'gcloud',
        'beta',
        'billing',
        'projects',
        'link',
        project_id,
        '--billing-account',
        billing_account_id,
    ]
    subprocess.check_output(command)
    logging.info(f'Assigned a billing account to {project_id}.')


def create_budget(project_id: str, amount=100):
    """
    Create a monthly budget for the project_id
    """
    command = [
        'gcloud',
        *('--project', BILLING_PROJECT_ID),
        *('billing', 'budgets', 'create'),
        *('--display-name', project_id),
        *('--billing-account', BILLING_ACCOUNT_ID),
        *('--filter-projects', f'projects/{project_id}'),
        *('--budget-amount', f'{amount:.2f}AUD'),
        *('--calendar-period', 'month'),
        *(
            '--threshold-rule=percent=0.5',
            '--threshold-rule=percent=0.9',
            '--threshold-rule=percent=1.0',
        ),
        *(
            '--notifications-rule-pubsub-topic',
            f'projects/{BILLING_PROJECT_ID}/topics/budget-notifications',
        ),
    ]

    return subprocess.check_output(command)


def get_hail_service_accounts(dataset: str):
    """Get hail service accounts from kubectl"""
    subprocess.check_output(
        [
            'gcloud',
            f'--project={HAIL_PROJECT}',
            'container',
            'clusters',
            'get-credentials',
            'vdc',
        ]
    )
    hail_tokens = subprocess.check_output(GET_HAIL_TOKENS(dataset), shell=True).decode()
    hail_client_emails_by_level = get_client_emails_from_kubectl_output(hail_tokens)

    return hail_client_emails_by_level


def create_hail_accounts(dataset):
    """
    Create 3 service accounts ${ds}-{test,standard,full} in Hail Batch
    """
    raise NotImplementedError

    # Based on: https://github.com/hail-is/hail/pull/11249
    with open(os.path.expanduser('~/.hail/tokens.json')) as f:
        hail_auth_token = json.load(f)['default']

    username_suffixes = ['-test', '-standard', '-full']
    for suffix in username_suffixes:
        username = dataset + suffix
        url = f'https://auth.hail.populationgenomics.org.au/api/v1alpha/user/{username}/create'
        post_resp = requests.post(
            url=url,
            headers={'Authorization': f'Bearer {hail_auth_token}'},
            data=json.dumps(
                {
                    'email': None,
                    'login_id': '',
                    'is_developer': False,
                    'is_service_account': True,
                }
            ),
        )
        print(post_resp)


def get_client_emails_from_kubectl_output(output):
    """
    Kubectl gives us 3 json blobs, so use a regex to find the hail emails
    """
    responses = RE_CLIENT_EMAIL_MATCHER.findall(output)
    if len(responses) != 3:
        raise ValueError(
            f'There was an error finding the hail client emails in the response, only found {responses} responses.'
        )
    return {'test': responses[0], 'standard': responses[1], 'full': responses[2]}


def generate_pulumi_stack_file(
    pulumi_config_fn: str,
    gcp_project: str,
    dataset: str,
    add_to_seqr_stack: bool,
    create_release_buckets: bool,
):
    """
    Generate Pulumi.{dataset}.yaml pulumi stack file, with required params
    """
    if os.path.exists(pulumi_config_fn):
        raise ValueError(
            'Can not create pulumi stack file as the pulumi config already exists'
        )

    current_branch = subprocess.check_output(
        ['git', 'rev-parse', '--abbrev-ref', 'HEAD']
    ).decode()
    if current_branch != 'main':
        answer = str(
            input(
                f'Expected branch to be "main", got "{current_branch}". '
                'Do you want to continue (and branch from this branch) (y/n)? '
            )
        ).lower()
        if answer not in TRUTHY_VALUES:
            raise SystemExit

    inp = str(
        input('Please confirm you have created the accounts in Hail Batch (y/n): ')
    )
    if inp not in TRUTHY_VALUES:
        raise SystemExit

    hail_client_emails_by_level = get_hail_service_accounts(dataset=dataset)

    formed_hail_config = {
        f'datasets:hail_service_account_{access_level}': value
        for access_level, value in hail_client_emails_by_level.items()
    }
    pulumi_stack = {
        'encryptionsalt': 'v1:SPQXlVggjxw=:v1:X1sTpViNuyK+Wcom:3GANI8gZOKtk0gG7BklsyHeNU5uVLw==',
        'config': {
            'datasets:archive_age': '90',
            'datasets:customer_id': 'C010ys3gt',
            'datasets:enable_release': create_release_buckets,
            'gcp:billing_project': gcp_project,
            'gcp:project': dataset,
            'gcp:user_project_override': 'true',
            **formed_hail_config,
        },
    }

    with open(pulumi_config_fn, 'w+', encoding='utf-8') as fp:
        print(f'Writing to {pulumi_config_fn}')
        yaml.dump(pulumi_stack, fp, default_flow_style=False)

    if add_to_seqr_stack:
        add_dataset_to_seqr_depends_on(dataset)

    logging.info('Preparing GIT commit')
    branch_name = f'add-{dataset}-stack'
    subprocess.check_output(['git', 'checkout', '-b', branch_name])
    subprocess.check_output(['git', 'add', pulumi_config_fn])

    if add_to_seqr_stack:
        subprocess.check_output(['git', 'add', 'Pulumi.seqr.yaml'])

    default_commit_message = f'Adds {dataset} dataset'
    commit_message = str(
        input(f'Commit message (default="{default_commit_message}"): ')
    )
    subprocess.check_output(
        ['git', 'commit', '-m', commit_message or default_commit_message]
    )
    logging.info(
        f'Created stack, you can push this WITH:\n\n'
        f'\tgit push --set-upstream origin {branch_name}'
    )


def generate_upload_account_json(dataset, gcp_project):
    """
    Generate access JSON for main-upload service account
    """
    service_account_fn = os.path.join(os.curdir, f'{dataset}-sa-upload.json')
    subprocess.check_output(
        [
            *('gcloud', 'iam', 'service-accounts', 'keys', 'create'),
            service_account_fn,
            f'--iam-account=main-upload@{gcp_project}.iam.gserviceaccount.com',
        ]
    )
    logging.info(f'Generated service account: {service_account_fn}')


def add_dataset_to_seqr_depends_on(dataset: str):
    """
    Add dataset to depends_on in seqr stack
    """
    with open('Pulumi.seqr.yaml', 'r+') as f:
        d = yaml.safe_load(f)
        config = d['config']
        config['datasets:depends_on'] = json.dumps(
            [*json.loads(config['datasets:depends_on']), dataset]
        )
        # go back to the start for writing to disk
        f.seek(0)
        yaml.dump(d, f, default_flow_style=False)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter)
    main()
