"""
Cromwell CLI
"""
# pylint: disable=too-many-arguments,too-many-return-statements
import os
import time
import json
import argparse
import subprocess
from shlex import quote
from typing import List, Dict, Optional, Any

import requests
import hailtop.batch as hb

from analysis_runner.constants import (
    CROMWELL_URL,
    ANALYSIS_RUNNER_PROJECT_ID,
    CROMWELL_AUDIENCE,
)
from analysis_runner.util import (
    logger,
    add_general_args,
    _perform_version_check,
    confirm_choice,
    get_google_identity_token,
    SERVER_ENDPOINT,
    get_server_config,
)
from analysis_runner.git import (
    get_git_default_remote,
    get_git_commit_ref_of_current_repository,
    get_repo_name_from_remote,
    get_relative_path_from_git_root,
)
from analysis_runner.cromwell_model import WorkflowMetadataModel

# lambda so we don't see ordering definition errors
# flake8: noqa
CROMWELL_MODES = lambda: {
    'submit': (_add_cromwell_submit_args_to, _run_cromwell),
    'status': (_add_cromwell_status_args, _check_cromwell_status),
    'visualise': (
        _add_cromwell_metadata_visualier_args,
        _visualise_cromwell_metadata_from_file,
    ),
}


def add_cromwell_args(parser=None) -> argparse.ArgumentParser:
    """Create / add arguments for cromwell argparser"""
    if not parser:
        parser = argparse.ArgumentParser('cromwell analysis-runner')

    subparsers = parser.add_subparsers(dest='cromwell_mode')

    for mode, (add_args, _) in CROMWELL_MODES().items():
        add_args(subparsers.add_parser(mode))

    return parser


def run_cromwell_from_args(args):
    """Run cromwell CLI mode from argparse.args"""
    cromwell_modes = CROMWELL_MODES()

    kwargs = vars(args)
    cromwell_mode = kwargs.pop('cromwell_mode')
    if cromwell_mode not in cromwell_modes:
        raise NotImplementedError(cromwell_mode)

    return cromwell_modes[cromwell_mode][1](**kwargs)


def _add_generic_cromwell_visualiser_args(parser: argparse.ArgumentParser):
    parser.add_argument('-l', '--expand-completed', default=False, action='store_true')
    parser.add_argument('--monochrome', default=False, action='store_true')


def _add_cromwell_status_args(parser: argparse.ArgumentParser):
    """Add cli args for checking status of Cromwell workflow"""
    parser.add_argument('workflow_id')
    parser.add_argument('--json-output', help='Output metadata to this path')

    _add_generic_cromwell_visualiser_args(parser)

    return parser


def _add_cromwell_metadata_visualier_args(parser: argparse.ArgumentParser):
    """
    Add arguments for visualising cromwell workflow from metadata file
    """
    parser.add_argument('metadata_file')
    _add_generic_cromwell_visualiser_args(parser)
    return parser


def _add_cromwell_submit_args_to(parser):
    """
    Add cli args for submitting WDL workflow to cromwell,
    via the analysis runner
    """

    add_general_args(parser)

    parser.add_argument(
        '-i',
        '--inputs',
        help='Relative path to input JSON',
        required=False,
        action='append',
    )
    # matches the cromwell param
    parser.add_argument(
        '-p',
        '--imports',
        nargs='+',
        required=False,
        help='A list of directories which are used to search for workflow imports',
    )
    parser.add_argument(
        '--workflow-input-prefix',
        type=str,
        required=False,
        help='Prefix to apply to all inputs AFTER the workflow argument, usually the workflow name',
    )

    parser.add_argument(
        '--labels',
        type=str,
        required=False,
        help='A json of labels to be applied to workflow.',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print curl request that would be sent to analysis-runner and exit',
    )

    # workflow WDL
    parser.add_argument('workflow', help='WDL file to submit to cromwell')

    # other inputs
    parser.add_argument(
        'dynamic_inputs',
        nargs=argparse.REMAINDER,
        help='--{key} {value} that get automatically parsed into an inputs json',
    )

    return parser


def _run_cromwell(
    dataset,
    output_dir,
    description,
    access_level,
    workflow: str,
    inputs: List[str],
    imports: List[str] = None,
    workflow_input_prefix: str = None,
    dynamic_inputs: List[str] = None,
    commit=None,
    repository=None,
    cwd=None,
    labels=None,
    dry_run=False,
):
    """
    Prepare parameters for cromwell analysis-runner job
    """
    if repository is not None and commit is None:
        raise Exception(
            "You must supply the '--commit <SHA>' parameter "
            "when specifying the '--repository'"
        )

    _perform_version_check()

    if access_level == 'full':
        if not confirm_choice(
            'Full access increases the risk of accidental data loss. Continue?',
        ):
            raise SystemExit()

    _repository = repository
    _commit_ref = commit
    _cwd = cwd

    if repository is None:
        _repository = get_repo_name_from_remote(get_git_default_remote())
        if _commit_ref is None:
            _commit_ref = get_git_commit_ref_of_current_repository()

        if _cwd is None:
            _cwd = get_relative_path_from_git_root()

    if _cwd == '.':
        _cwd = None

    _inputs_dict = None
    if dynamic_inputs:
        _inputs_dict = parse_additional_args(dynamic_inputs)
        if workflow_input_prefix:
            _inputs_dict = {
                workflow_input_prefix + k: v for k, v in _inputs_dict.items()
            }

    _labels = None
    if labels:
        _labels = json.loads(labels)

    body = {
        'dataset': dataset,
        'output': output_dir,
        'repo': _repository,
        'accessLevel': access_level,
        'commit': _commit_ref,
        'description': description,
        'cwd': _cwd,
        'workflow': workflow,
        'inputs_dict': _inputs_dict,
        'input_json_paths': inputs or [],
        'dependencies': imports or [],
        'labels': _labels,
    }

    if dry_run:
        logger.warning('Dry-run, printing curl and exiting')
        curl = f"""\
curl --location --request POST \\
    '{SERVER_ENDPOINT}/cromwell' \\
    --header "Authorization: Bearer $(gcloud auth print-identity-token)" \\
    --header "Content-Type: application/json" \\
    --data-raw '{json.dumps(body, indent=4)}'"""

        print(curl)
        return

    response = requests.post(
        SERVER_ENDPOINT + '/cromwell',
        json=body,
        headers={'Authorization': f'Bearer {get_google_identity_token()}'},
    )
    try:
        response.raise_for_status()
        logger.info(f'Request submitted successfully: {response.text}')
    except requests.HTTPError as e:
        logger.critical(
            f'Request failed with status {response.status_code}: {str(e)}\n'
            f'Full response: {response.text}',
        )


def _check_cromwell_status(workflow_id, json_output: Optional[str], *args, **kwargs):
    """Check cromwell status with workflow_id"""

    url = SERVER_ENDPOINT + f'/cromwell/{workflow_id}/metadata'

    response = requests.get(
        url, headers={'Authorization': f'Bearer {get_google_identity_token()}'}
    )
    response.raise_for_status()
    d = response.json()

    if json_output:
        logger.info(f'Writing metadata to: {json_output}')
        with open(json_output, 'w+', encoding='utf-8') as f:
            json.dump(d, f)

    model = WorkflowMetadataModel.parse(d)
    print(model.display(*args, **kwargs))


def _visualise_cromwell_metadata_from_file(metadata_file: str, *args, **kwargs):
    """Visualise cromwell metadata progress from a json file"""
    with open(metadata_file, encoding='utf-8') as f:
        model = WorkflowMetadataModel.parse(json.load(f))

    visualise_cromwell_metadata(model, *args, **kwargs)


def visualise_cromwell_metadata(
    model: WorkflowMetadataModel, expand_completed, monochrome
):
    """Print the visualisation of cromwell metadata model"""
    print(model.display(expand_completed=expand_completed, monochrome=monochrome))


def try_parse_value(value: Optional[str]):
    """Try parse value from command line string"""
    if value is None or value == 'None' or value == 'null':
        return value

    if isinstance(value, list):
        return list(map(try_parse_value, value))

    if not isinstance(value, str):
        # maybe the CLI did some parsing
        return value

    if value == 'true':
        return True
    if value == 'false':
        return False

    # EAFP
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass

    return value


def parse_keyword(keyword: str):
    """Parse CLI keyword"""
    return keyword[2:].replace('-', '_')


def parse_additional_args(args: List[str]) -> Dict[str, any]:
    """
    Parse a list of strings to an inputs json

    The theory is to look for any args that start with '--',
    strip those two leading dashes, replace '-' with '_'.
    1. If the keyword is followed by another keyword, then make
          the first param a boolean (with True)
    2. If the keyword is followed by a single non-keyword value, set the value
          parse the type and
    3. If the keyword is followed by multiple arguments, make it an array

    Examples:
    >>> parse_additional_args(['--keyword1', '--keyword2'])
    {'keyword1': True, 'keyword2': True}

    >>> parse_additional_args(['--keyword', 'single value'])
    {'keyword': 'single value'}

    >>> parse_additional_args(['--keyword', 'val1', 'val2'])
    {'keyword': ['val1', 'val2']}

    >>> parse_additional_args(['--keyword', 'false'])
    {'keyword': False}


    Special cases:
        * Keyword with single value, followed by the same keyword with no params, turn the original list into an array, eg:

    >>> parse_additional_args(['--keyword', 'value', '--keyword'])
    {'keyword': ['value']}

    >>> parse_additional_args(['--keyword', 'value1', 'value2', '--keyword'])
    {'keyword': [['value1', 'value2']}

        * Keyword with multiple values, followed by same keyword with multiple values results in nested lists

    >>> parse_additional_args(['--keyword', 'val1_a', 'val1_b', '--keyword', 'val2_a', 'val2_b'])
    {'keyword': [['val1_a', 'val1_b'], ['val2_a', 'val2_b']]}
    """

    keywords = {}

    def add_keyword_value_to_keywords(keyword, value):
        if keyword in keywords:
            if value is None:
                value = [keywords.get(keyword)]
            else:
                value = [keywords.get(keyword), try_parse_value(new_value)]
        elif value is None:
            # flag
            value = True
        else:
            value = try_parse_value(value)

        keywords[keyword] = value

    current_keyword = None
    new_value: any = None

    for arg in args:
        if not arg.startswith('--'):

            if new_value is None:
                # if it's the first value we're seeing, set it
                new_value = arg
            elif not isinstance(new_value, list):
                # if it's the second value we're seeing
                new_value = [new_value, arg]
            else:
                # 3rd or more, just keep adding it to an array
                new_value.append(arg)

            continue

        # found a new keyword
        if current_keyword is None:
            # first case, do nothing
            current_keyword = parse_keyword(arg)
            continue

        add_keyword_value_to_keywords(current_keyword, new_value)
        current_keyword = parse_keyword(arg)
        new_value = None

    add_keyword_value_to_keywords(current_keyword, new_value)

    return keywords


def run_cromwell_workflow(
    job: hb.batch.job.Job,
    dataset: str,
    access_level: str,
    workflow: str,
    cwd: Optional[str],
    libs: List[str],
    output_suffix: str,
    labels: Dict[str, str]=None,
    input_dict: Optional[Dict[str, Any]] = None,
    input_paths: List[str] = None,
    server_config: Dict[str, Any] = None,
):
    from cpg_utils.cloud import read_secret

    def get_cromwell_key(dataset, access_level):
        """Get Cromwell key from secrets"""
        secret_name = f'{dataset}-cromwell-{access_level}-key'
        return read_secret(ANALYSIS_RUNNER_PROJECT_ID, secret_name)

    if cwd:
        job.command(f'cd {quote(cwd)}')

    deps_path = None
    if libs:
        deps_path = 'tools.zip'
        job.command('zip -r tools.zip ' + ' '.join(quote(s + '/') for s in libs))

    cromwell_post_url = CROMWELL_URL + '/api/workflows/v1'

    google_labels = {}

    if labels:
        google_labels.update(labels)

    google_labels.update({'compute-category': 'cromwell'})

    if not server_config:
        server_config = get_server_config()

    ds_config = server_config[dataset]
    project = ds_config.get('projectId')
    service_account_json = get_cromwell_key(dataset=dataset, access_level=access_level)
    # use the email specified by the service_account_json again
    service_account_dict = json.loads(service_account_json)
    service_account_email = service_account_dict.get('client_email')
    if not service_account_email:
        raise ValueError("The service_account didn't contain an entry for client_email")

    if access_level == 'test':
        intermediate_dir = f'gs://cpg-{dataset}-test-tmp/cromwell'
        workflow_output_dir = f'gs://cpg-{dataset}-test/{output_suffix}'
    else:
        intermediate_dir = f'gs://cpg-{dataset}-main-tmp/cromwell'
        workflow_output_dir = f'gs://cpg-{dataset}-main/{output_suffix}'

    workflow_options = {
        'user_service_account_json': service_account_json,
        'google_compute_service_account': service_account_email,
        'google_project': project,
        'jes_gcs_root': intermediate_dir,
        'final_workflow_outputs_dir': workflow_output_dir,
        'google_labels': google_labels,
    }

    input_paths = input_paths or []
    if input_dict:
        tmp_input_json_path = '/tmp/inputs.json'
        job.command(f"echo '{json.dumps(input_dict)}' > {tmp_input_json_path}")
        input_paths.append(tmp_input_json_path)

    inputs_cli = []
    for idx, value in enumerate(input_paths):
        key = 'workflowInputs'
        if idx > 0:
            key += f'_{idx + 1}'

        inputs_cli.append(f'-F "{key}=@{value}"')

    output_workflow_id = job.out_workflow_id
    job.command(
        f"""
    echo '{json.dumps(workflow_options)}' > workflow-options.json
    access_token=$(gcloud auth print-identity-token --audiences={CROMWELL_AUDIENCE})
    wid=$(curl -X POST "{cromwell_post_url}" \\
    -H "Authorization: Bearer $access_token" \\
    -H "accept: application/json" \\
    -H "Content-Type: multipart/form-data" \\
    -F "workflowSource=@{workflow}" \\
    {' '.join(inputs_cli)} \\
    -F "workflowOptions=@workflow-options.json;type=application/json" \\
    {f'-F "workflowDependencies=@{deps_path}"' if deps_path else ''})

    echo "Submitted workflow with ID $wid"
    echo $wid | jq -r .id >> {output_workflow_id}
    """
    )

    return output_workflow_id


class CromwellError(Exception):
    pass


def watch_workflow_and_get_output(
    b: hb.Batch,
    job_prefix: str,
    workflow_id_file,
    outputs_to_collect: Dict[str, Optional[int]],
    driver_image: Optional[str]=None,
):
    """
    This is a little bit tricky, but the process is:

    - Wait for a cromwell workflow to finish,
    - If it succeeds, get the outputs (as a json)
    - (Hard) Get the value of the output back into Hail Batch as a resource file.

    Getting the value of the output back into hail batch because the:
        - outputs to collect +
        - number of outputs to collect must be known up-front.

    So unfortunately, this function needs to know the structure of the outputs you
    want to collect. It currently only supports:
        - a single value, or
        - a list of values

    If the starts with "gs://", we'll copy it as a resource file,
    otherwise write the value into a file which will be a batch resource.
    """

    def watch_workflow(workflow_id_file) -> Dict[str, any]:

        with open(workflow_id_file, encoding='utf-8') as f:
            workflow_id = f.read().strip()
        print(f'Got workflow ID: {workflow_id}')
        final_statuses = {'failed', 'aborted'}
        subprocess.check_output(['gcloud', '-q', 'auth', 'activate-service-account', '--key-file=/gsa-key/key.json'])
        url = f'https://cromwell.populationgenomics.org.au/api/workflows/v1/{workflow_id}/status'
        exception_count = 50
        while True:
            if exception_count <= 0:
                raise CromwellError('Unreachable')
            try:
                token = get_cromwell_oauth_token()
                r = requests.get(url, headers={'Authorization': f'Bearer {token}'})
                if not r.ok:
                    exception_count -= 1
                    time.sleep(30)
                    print(f'Got not okay from cromwell: {r.text}')
                    continue
                status = r.json().get('status')
                print(f'Got cromwell status: {status}')
                exception_count = 50
                if status.lower() == 'succeeded':
                    # process outputs here
                    outputs_url = f'https://cromwell.populationgenomics.org.au/api/workflows/v1/{workflow_id}/outputs'
                    res2 = requests.get(outputs_url, headers={'Authorization': f'Bearer {token}'})
                    if not res2.ok:
                        print('Received error when fetching cromwell outputs, will retry in 15 seconds')
                        continue
                    outputs = res2.json()
                    print(f'Got outputs: {outputs}')
                    return outputs.get('outputs')
                if status.lower() in final_statuses:
                    raise CromwellError(status)
                time.sleep(15)
            except Exception as e:
                exception_count -= 1
                print(f'Got worse exception: {e}')
                time.sleep(30)

    watch_job = b.new_python_job(job_prefix + "_watch")
    _driver_image = driver_image or os.getenv('DRIVER_IMAGE')

    watch_job.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
    watch_job.env('PYTHONUNBUFFERED', '1')
    watch_job.image(_driver_image)

    rdict = watch_job.call(watch_workflow, workflow_id_file).as_json()
    out_file_map = {}
    for output, n_outputs in outputs_to_collect.items():

        if n_outputs is None:
            j = b.new_job(f'{job_prefix}_collect_{output}')
            out_file_map[output] = _copy_file_into_batch(
                j=j,
                rdict=rdict,
                output=output,
                idx=None,
                output_filename=j.out,
            )
        else:
            out_file_map[output] = []
            for idx in range(n_outputs):
                j = b.new_job(f'{job_prefix}_collect_{output}')
                out_file_map[output].append(
                    _copy_file_into_batch(
                        j=j,
                        rdict=rdict,
                        output=output,
                        idx=idx,
                        output_filename=j.out,
                    )
                )

    return out_file_map


def _copy_file_into_batch(
    j: hb.batch.job.Job, *, rdict, output, idx: Optional[int], output_filename
):

    if idx is None:
        error_description = output
        jq_el = f'"{output}"'
    else:
        error_description = f'{output}[{idx}]'
        jq_el = f'"{output}"[{idx}]'

    j.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
    j.command('gcloud -q auth activate-service-account --key-file=/gsa-key/key.json')

    j.command(
        f"""
OUTPUT_TYPE=$(cat {rdict} | jq '.{jq_el}' | jq -r type)
if [ $OUTPUT_TYPE != "string" ]; then
    echo "The element {error_description} was not of type string, got $OUTPUT_TYPE";
    # exit 1;
fi

OUTPUT_VALUE=$(cat {rdict} | jq -r '.{jq_el}')
if [[ "$OUTPUT_VALUE" == gs://* ]]; then
    echo "Copying file from $OUTPUT_VALUE";
    gsutil cp $OUTPUT_VALUE {output_filename};
else
    echo "$OUTPUT_VALUE" > {output_filename}
fi
    """
    )

    return output_filename


def get_cromwell_oauth_token():
    token_command = [
        'gcloud',
        'auth',
        'print-identity-token',
        f'--audiences={CROMWELL_AUDIENCE}',
    ]
    token = subprocess.check_output(token_command).decode().strip()
    return token
