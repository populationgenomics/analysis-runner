import asyncio
import json
import sys

import click
import requests
from google.cloud import storage

from cpg_utils import to_path
from cpg_utils.constants import CROMWELL_URL
from cpg_utils.cromwell import get_cromwell_oauth_token
from metamist.apis import AnalysisApi, ParticipantApi
from metamist.models import Analysis, AnalysisStatus


def get_workflow_metadata_from_file(workflow_metadata_file_path: str):
    try:
        with open(workflow_metadata_file_path) as f:
            return json.load(f)
    except FileNotFoundError as e:
        print(f'Error opening workflow metadata file: {e!r}')
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f'Error decoding workflow metadata JSON: {e!r}')
        sys.exit(1)


def get_workflow_metadata_from_api(workflow_id: str):
    url = f'{CROMWELL_URL}/api/workflows/v1/{workflow_id}/metadata'
    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {get_cromwell_oauth_token()}',
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error fetching workflow metadata: {e!r}')
        sys.exit(1)


def print_parsed_workflow_summary(
    wf_id: str,
    sg_id: str,
    dataset: str,
    status_dict: dict,
    outputs: dict,
):
    """Prints the execution status and outputs of the sub-workflows."""
    print(f'\n{dataset} :: {sg_id} :: {wf_id} :: Workflow Summary:\n')
    for subworkflow in [
        'CollectCounts',
        'CollectSVEvidence',
        'Scramble',
        'Whamg',
        'Manta',
    ]:
        print(f'  {subworkflow}:')
        if not status_dict.get(subworkflow):
            print('    No attempts found')
            continue

        execution_status = status_dict[subworkflow]
        if isinstance(execution_status, dict):
            print(f'    {len(execution_status)} attempt(s):')
            for attempt, status in execution_status.items():
                print(f'      {attempt}: {status}')
                continue
        else:
            print(f'    {execution_status}')
            continue

        if not outputs.get(subworkflow):
            print('    No outputs found')
            continue
        print('    Outputs:')
        for key, value in outputs[subworkflow].items():
            print(f'      {key}: {value}')
    print()


def parse_subworkflow_status_and_outputs(
    subworkflow: str,
    attempts: list[dict],
) -> tuple[dict, dict]:
    """
    Parse the status and outputs of a sub-workflow from the Cromwell metadata JSON.
    """
    status: dict = {}
    outputs: dict[str, dict] = {}
    subworkflow_name = subworkflow.split('.')[-1]
    if not attempts:
        status[subworkflow_name] = 'Not Started'
        return status, outputs

    # The attempts dict is a list of attempts, check each to recover the outputs
    # and status of the sub-workflow. The final status is the status of the last
    # attempt, which is the most relevant.
    status[subworkflow_name] = {}
    for attempt_no in range(len(attempts)):
        assert attempts[attempt_no].get('attempt') == attempt_no + 1
        # Get the execution status
        execution_status = attempts[attempt_no].get('executionStatus')
        # Check for failures
        if 'failures' in attempts[attempt_no]:
            execution_status = 'Failed'
            failure_message = attempts[attempt_no]['failures'][0].get(
                'message',
                'Unknown failure',
            )
            status[subworkflow_name][f'attempt {attempt_no + 1}'] = (
                f'{execution_status}: {failure_message}'
            )
        else:
            status[subworkflow_name][f'attempt {attempt_no + 1}'] = execution_status

        # Get the outputs of the sub-workflow
        outputs[subworkflow_name] = outputs.get(subworkflow_name, {})
        if workflow_outputs := attempts[attempt_no].get('outputs', {}):
            for output_key, output_value in workflow_outputs.items():
                if output_key == 'is_dragen_3_7_8':
                    # Skip the is_dragen_3_7_8 output
                    continue
                if output_value.endswith(('cram', 'crai')):
                    continue
                outputs[subworkflow_name][output_key] = output_value

    return status, outputs


def parse_workflow_status_and_outputs(wf_id: str, json_data: dict):
    """
    Parse the status and outputs of all subworkflows within a workflow from the Cromwell metadata JSON.
    """
    sg_id = None
    dataset = None
    status = {}
    outputs: dict[str, dict] = {}
    calls = json_data.get('calls', {})

    for subworkflow, attempts in calls.items():
        # Get the dataset and sequencing group ID from the LocalizeReads sub-workflow
        # This should never be missing as it is required for all other sub-workflows
        if subworkflow == 'GatherSampleEvidence.LocalizeReads':
            input_reads = attempts[0].get('inputs', {}).get('reads_path')
            sg_id = input_reads.split('/')[-1].split('.')[0]
            dataset = input_reads.removeprefix('gs://cpg-').rsplit('-', 1)[0]
            continue
        if subworkflow.startswith('GatherSampleEvidence.'):
            subworkflow_status, subworkflow_outputs = (
                parse_subworkflow_status_and_outputs(
                    subworkflow,
                    attempts,
                )
            )
            status.update(subworkflow_status)
            outputs.update(subworkflow_outputs)

    if not sg_id:
        raise ValueError('SG ID not found in metadata')
    print_parsed_workflow_summary(wf_id, sg_id, dataset, status, outputs)
    return {sg_id.upper(): {'dataset': dataset, 'status': status, 'outputs': outputs}}


def copy_outputs_to_bucket(
    sg_id: str,
    dataset: str,
    outputs: dict,
    source_bucket_name: str,
    destination_bucket_name: str,
    dry_run: bool = False,
):
    """Copy outputs to the sv_evidence folder of the dataset's main bucket, and saves the file sizes in a dictionary."""
    analysis_file_sizes = {}
    storage_client = storage.Client()

    source_bucket = storage_client.bucket(source_bucket_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    print(f'{dataset} :: {sg_id} :: Copying outputs summary:')
    print(f'  Destination: gs://{destination_bucket_name}/sv_evidence/\n')
    for _, output in outputs.items():
        for value in output.values():
            if value.endswith('scramble.vcf.gz'):
                analysis_file_sizes['scramble'] = to_path(value).stat().st_size
            elif value.endswith('wham.vcf.gz'):
                analysis_file_sizes['wham'] = to_path(value).stat().st_size
            elif value.endswith('manta.vcf.gz'):
                analysis_file_sizes['manta'] = to_path(value).stat().st_size
            blob_name = value.replace(f'gs://{source_bucket_name}/', '')
            source_blob = source_bucket.blob(blob_name)
            destination_blob_name = (
                f'sv_evidence/{blob_name.split("/")[-1]}'  # Copy to sv_evidence folder
            )
            destination_gs_url = (
                f'gs://{destination_bucket_name}/{destination_blob_name}'
            )
            destination_gs_url = destination_gs_url.replace(
                'counts.tsv.gz',
                'coverage_counts.tsv.gz',
            )
            if not dry_run:
                print(f'    Copying {source_blob.name} to {destination_gs_url}')
                blob_copy = source_bucket.copy_blob(
                    source_blob,
                    destination_bucket,
                    destination_blob_name,
                )
                print(f'    Blob {blob_copy.name} copied')
            else:
                print(
                    f'    DRY RUN: Would have copied gs://{source_bucket_name}/{source_blob.name} to {destination_gs_url}',
                )
            print()

    return analysis_file_sizes


def get_analyses_to_create(
    sg_id: str,
    participant_eid: str,
    dataset: str,
    analysis_file_sizes: dict,
):
    """Queues the analyses to be created."""
    analyses = []
    print(f'{dataset} :: {sg_id} :: {participant_eid} Analyses summary:\n')
    for analysis_type in ['scramble', 'wham', 'manta']:
        if analysis_file_sizes.get(analysis_type) is None:
            print(f'  {analysis_type}: No outputs found. Skipping...')
            continue
        output_path = to_path(
            f'gs://cpg-{dataset}-main/sv_evidence/{sg_id}.{analysis_type}.vcf.gz',
        )
        print(
            f'  {analysis_type}: {output_path.name} : {analysis_file_sizes[analysis_type]} bytes',
        )
        sv_analysis = Analysis(
            type='sv',
            output=str(output_path),
            meta={
                'stage': 'GatherSampleEvidence',
                'sequencing_type': 'genome',
                'dataset': dataset,
                'sequencing_group': sg_id,
                'participant_id': participant_eid,
                'size': analysis_file_sizes[analysis_type],
            },
            status=AnalysisStatus('completed'),
            sequencing_group_ids=[sg_id],
        )
        analyses.append(sv_analysis)

    return {sg_id: analyses}


def get_sgid_peid_map(datasets: list[str]):
    """Synchronous entrypoint to get the mapping of sequencing group IDs to participant EIDs for the given datasets."""
    return asyncio.get_event_loop().run_until_complete(
        get_sgid_peid_map_async(datasets),
    )


async def get_sgid_peid_map_async(datasets: list[str]):
    """Get the mapping of sequencing group IDs to participant EIDs for the given datasets."""
    papi = ParticipantApi()
    sg_peid_map = {}
    promises = []
    for dataset in datasets:
        promises.append(
            papi.get_external_participant_id_to_sequencing_group_id_async(
                dataset,
                sequencing_type='genome',
                flip_columns=True,
            ),
        )

    results = await asyncio.gather(*promises)
    for result in results:
        for sgid, peid in result:
            sg_peid_map[sgid] = peid
    return sg_peid_map


def create_sv_analyses(sg_datasets: dict, sg_analyses: dict):
    """Synchronous entrypoint to create analyses for the given sequencing groups and datasets."""
    asyncio.get_event_loop().run_until_complete(
        create_sv_analyses_async(sg_datasets, sg_analyses),
    )


async def create_sv_analyses_async(sg_datasets: dict, sg_analyses: dict):
    """
    Asynchronously create analyses for the given sequencing groups and datasets.
    """
    aapi = AnalysisApi()
    promises = []
    for sg_id, dataset in sg_datasets.items():
        for analysis in sg_analyses[sg_id]:
            promises.append(
                aapi.create_analysis_async(project=dataset, analysis=analysis),
            )

    await asyncio.gather(*promises)


@click.command()
@click.option('--dataset', required=True, help='Dataset name(s)', multiple=True)
@click.option(
    '--workflow-id',
    required=True,
    help='Cromwell workflow ID',
    multiple=True,
)
@click.option('--dry-run', is_flag=True, help='Dry run mode')
def main(dataset: list[str], workflow_id: list[str], dry_run: bool = False):
    """
    A script to parse Cromwell workflow metadata and collect any successful SV outputs.
    Copies the outputs to the dataset's main bucket and creates analyses for the successful sub-workflows.

    The sub-workflows are identified by the GatherSampleEvidence prefix in their names:
    GatherSampleEvidence.scramble, GatherSampleEvidence.wham, GatherSampleEvidence.manta.
    """
    sg_peid_map = get_sgid_peid_map(dataset)

    sg_analyses_sizes = {}
    sg_datasets = {}
    sg_analyses = {}
    print(f'Parsing {len(workflow_id)} workflows...')
    for wf_id in workflow_id:
        json_data = get_workflow_metadata_from_api(wf_id)
        workflow_results = parse_workflow_status_and_outputs(wf_id, json_data)

        sg_id = next(iter(workflow_results.keys()))
        wf_dataset = workflow_results[sg_id]['dataset']
        sg_datasets[sg_id] = wf_dataset
        outputs = workflow_results[sg_id]['outputs']

        # Copy outputs to bucket
        source_bucket_name = 'cpg-seqr-main-tmp'
        destination_bucket_name = f'cpg-{wf_dataset}-main'
        analysis_file_sizes = copy_outputs_to_bucket(
            sg_id,
            wf_dataset,
            outputs,
            source_bucket_name,
            destination_bucket_name,
            dry_run=dry_run,
        )

        sg_analyses_sizes[sg_id] = analysis_file_sizes
        participant_eid = sg_peid_map[sg_id]
        analyses_to_create = get_analyses_to_create(
            sg_id,
            participant_eid,
            wf_dataset,
            analysis_file_sizes,
        )
        sg_analyses.update(analyses_to_create)

    if not dry_run:
        create_sv_analyses(sg_datasets, sg_analyses)
    else:
        print()
        for sg_id, sg_dataset in sg_datasets.items():
            print(
                f'{sg_dataset} :: {sg_id} :: DRY RUN: Would have created: {len(sg_analyses[sg_id])} SV analyses',
            )


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
