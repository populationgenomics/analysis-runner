"""
Cromwell module contains helper code for submitting + watching
jobs from within Hail batch.
"""
# pylint: disable=too-many-arguments,too-many-return-statements,broad-except
import json
import math
import os
import subprocess
import time
from datetime import datetime
from shlex import quote
from typing import List, Dict, Optional, Any

import hailtop.batch as hb
import requests
from cpg_utils.cloud import read_secret

from analysis_runner.constants import (
    CROMWELL_URL,
    ANALYSIS_RUNNER_PROJECT_ID,
    CROMWELL_AUDIENCE,
    GCLOUD_ACTIVATE_AUTH,
)
from analysis_runner.git import (
    get_git_default_remote,
    get_git_commit_ref_of_current_repository,
    get_repo_name_from_remote,
    prepare_git_job,
)
from analysis_runner.util import (
    logger,
    get_server_config,
)


def run_cromwell_workflow(
    job: hb.batch.job.Job,
    dataset: str,
    access_level: str,
    workflow: str,
    cwd: Optional[str],
    libs: List[str],
    output_suffix: str,
    labels: Dict[str, str] = None,
    input_dict: Optional[Dict[str, Any]] = None,
    input_paths: List[str] = None,
    server_config: Dict[str, Any] = None,
):
    """
    Run a cromwell workflow, and return a Batch.ResourceFile
    that contains the workflow ID
    """

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


def run_cromwell_workflow_from_repo_and_get_outputs(
    b: hb.Batch,
    job_prefix: str,
    dataset: str,
    access_level,
    workflow: str,
    outputs_to_collect: Dict[str, Optional[int]],
    libs: List[str],
    output_suffix: str,
    labels: Dict[str, str] = None,
    input_dict: Optional[Dict[str, Any]] = None,
    input_paths: List[str] = None,
    repo: Optional[str] = None,
    commit: Optional[str] = None,
    cwd: Optional[str] = None,
    driver_image: Optional[str] = None,
    server_config: Dict[str, Any] = None,
):
    """
    This function needs to know the structure of the outputs you
    want to collect. It currently only supports:
        - a single value, or
        - a list of values

    Eg: outputs_to_collect={
        'hello.out': None, # singleoutput
        'hello.outs': 5, # array output of length=5
    }

    If the starts with "gs://", we'll copy it as a resource file,
    otherwise write the value into a file which will be a batch resource.
    """
    _driver_image = driver_image or os.getenv('DRIVER_IMAGE')

    submit_job = b.new_job(f'{job_prefix}_submit')
    submit_job.image(_driver_image)
    prepare_git_job(
        job=submit_job,
        repo_name=(repo or get_repo_name_from_remote(get_git_default_remote())),
        commit=(commit or get_git_commit_ref_of_current_repository()),
        is_test=access_level == 'test',
    )

    workflow_id_file = run_cromwell_workflow(
        job=submit_job,
        dataset=dataset,
        access_level=access_level,
        workflow=workflow,
        cwd=cwd,
        libs=libs,
        output_suffix=output_suffix,
        input_dict=input_dict,
        input_paths=input_paths,
        labels=labels,
        server_config=server_config,
    )

    outputs_dict = watch_workflow_and_get_output(
        b,
        job_prefix=job_prefix,
        workflow_id_file=workflow_id_file,
        outputs_to_collect=outputs_to_collect,
        driver_image=_driver_image,
    )

    return outputs_dict


class CromwellError(Exception):
    """Cromwell status error"""


def watch_workflow_and_get_output(
    b: hb.Batch,
    job_prefix: str,
    workflow_id_file,
    outputs_to_collect: Dict[str, Optional[int]],
    driver_image: Optional[str] = None,
    max_poll_interval=60,  # 1 minute
    exponential_decrease_seconds=1200,  # 20 minutes
    max_sequential_exception_count=25,
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

    :param driver_image: If specified, must contain python3 (w/ requests), gcloud, jq
    """

    _driver_image = driver_image or os.getenv('DRIVER_IMAGE')
    start = datetime.now()

    def get_wait_interval():
        """
        Get wait time between 5s and {max_poll_interval},
        curved between 0s and {exponential_decrease_seconds}.
        """
        factor = (datetime.now() - start).total_seconds() / exponential_decrease_seconds
        if factor > 1:
            return max_poll_interval
        return max(5, int((1 - math.cos(math.pi * factor)) * max_poll_interval // 2))

    def watch_workflow(workflow_id_file) -> Dict[str, any]:
        """Python function to watch workflow, and return outputs"""
        with open(workflow_id_file, encoding='utf-8') as f:
            workflow_id = f.read().strip()
        logger.info(f'Received workflow ID: {workflow_id}')
        final_statuses = {'failed', 'aborted'}
        subprocess.check_output(GCLOUD_ACTIVATE_AUTH, shell=True)
        url = f'https://cromwell.populationgenomics.org.au/api/workflows/v1/{workflow_id}/status'
        _remaining_exceptions = max_sequential_exception_count
        while True:
            if _remaining_exceptions <= 0:
                raise CromwellError('Unreachable')
            try:
                token = get_cromwell_oauth_token()
                r = requests.get(url, headers={'Authorization': f'Bearer {token}'})
                if not r.ok:
                    _remaining_exceptions -= 1
                    wait_time = get_wait_interval()
                    logger.warning(
                        f'Received "not okay" (status={r.status_code}) from cromwell '
                        f'(waiting={wait_time}): {r.text}'
                    )
                    time.sleep(wait_time)
                    continue
                status = r.json().get('status')
                _remaining_exceptions = max_sequential_exception_count
                if status.lower() == 'succeeded':
                    logger.info(f'Cromwell workflow moved to succeeded state')
                    # process outputs here
                    outputs_url = f'https://cromwell.populationgenomics.org.au/api/workflows/v1/{workflow_id}/outputs'
                    r_outputs = requests.get(
                        outputs_url, headers={'Authorization': f'Bearer {token}'}
                    )
                    if not r_outputs.ok:
                        logger.warning(
                            'Received error when fetching cromwell outputs, will retry in 15 seconds'
                        )
                        continue
                    outputs = r_outputs.json()
                    logger.info(f'Received outputs from Cromwell: {outputs}')
                    return outputs.get('outputs')
                if status.lower() in final_statuses:
                    logger.error(f'Got failed cromwell status: {status}')
                    raise CromwellError(status)
                wait_time = get_wait_interval()
                logger.info(f'Got cromwell status: {status} (sleeping={wait_time})')
                time.sleep(wait_time)
            except Exception as e:
                _remaining_exceptions -= 1
                wait_time = get_wait_interval()
                logger.error(
                    f'Cromwell status watch caught general exception (sleeping={wait_time}): {e}'
                )
                time.sleep(wait_time)

    watch_job = b.new_python_job(job_prefix + '_watch')

    watch_job.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
    watch_job.env('PYTHONUNBUFFERED', '1')  # makes the logs go quicker
    watch_job.image(_driver_image)  # need an image with python3 + requests

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
                j = b.new_job(f'{job_prefix}_collect_{output}[{idx}]')
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
    """
    1. Take the file-pointer to the dictionary `rdict`,
    2. the output name `output`,
    3. check that the value we select is a string,
    4. either:
        (a) gsutil cp it into `output_filename`
        (b) write the value into `output_filename`
    """
    if idx is None:
        # if no index, select the value as-is
        error_description = output
        # wrap this in quotes, because output often contains a '.', which has to be escaped in jq
        jq_el = f'"{output}"'
    else:
        # if we're supplied an index, grab the value, then get the index, eg: '.hello[5]'
        error_description = f'{output}[{idx}]'
        # wrap this in quotes, because output often contains a '.', which has to be escaped in jq
        jq_el = f'"{output}"[{idx}]'

    # activate to gsutil cp
    j.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
    j.command(GCLOUD_ACTIVATE_AUTH)

    # this has to be in bash unfortunately :(
    # we want to check that the output we get is a string
    # if it starts with gs://, then we'll `gsutil cp` it into output_filename
    # otherwise write the value into output_filename.

    # in future, add s3://* or AWS handling here

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
    # cleaner to directly pipe into file
    cat {rdict} | jq -r '.{jq_el}' > {output_filename}
fi
    """
    )

    return output_filename


def get_cromwell_oauth_token():
    """Get oath token for cromwell, specific to audience"""
    token_command = [
        'gcloud',
        'auth',
        'print-identity-token',
        f'--audiences={CROMWELL_AUDIENCE}',
    ]
    token = subprocess.check_output(token_command).decode().strip()
    return token
