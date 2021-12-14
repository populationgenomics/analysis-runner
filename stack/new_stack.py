"""Create GCP project + stack file for Pulumi"""
import os
import re
from subprocess import check_output

import yaml
import click

ORGANIZATION_ID = 648561325637
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
@click.option('--release', required=False, is_flag=True)
def main(
    dataset: str,
    gcp_project: str = None,
    release=False,
):
    """Function that coordinates creating a project"""
    _gcp_project = gcp_project or dataset

    if len(dataset) > 17:
        raise ValueError(
            f'The dataset length must be less than 17 characters (got {len(dataset)})'
        )

    pulumi_config_fn = f'Pulumi.{dataset}.yaml'
    if os.path.exists(pulumi_config_fn):
        raise ValueError('The pulumi config already exists')

    inp = str(
        input('Please confirm you have created the accounts in Hail Batch (y/n): ')
    )
    if inp not in ('y', '1', 't'):
        raise SystemExit

    create_project(project_id=_gcp_project)
    assign_billing_account(_gcp_project)
    create_budget(_gcp_project)
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
            'datasets:enable_release': release,
            'gcp:billing_project': _gcp_project,
            'gcp:project': dataset,
            'gcp:user_project_override': 'true',
            **formed_hail_config,
        },
    }

    with open(pulumi_config_fn, 'w+', encoding='utf-8') as fp:
        print(f'Writing to {pulumi_config_fn}')
        yaml.dump(pulumi_stack, fp, default_flow_style=False)

    command = f"""\
    export PULUMI_CONFIG_PASSPHRASE=
    pulumi stack init {dataset}
    pulumi stack select {dataset}
    pulumi up
    """
    print(f'Created stack for {dataset}, can deploy with:\n{command}')


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
    check_output(command)


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
    check_output(command)


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

    return check_output(command)


def get_hail_service_accounts(dataset: str):
    """Get hail service accounts from kubectl"""
    check_output(
        [
            'gcloud',
            f'--project={HAIL_PROJECT}',
            'container',
            'clusters',
            'get-credentials',
            'vdc',
        ]
    )
    hail_tokens = check_output(GET_HAIL_TOKENS(dataset), shell=True).decode()
    hail_client_emails_by_level = get_client_emails_from_kubectl_output(hail_tokens)

    return hail_client_emails_by_level


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


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter)
    main()
