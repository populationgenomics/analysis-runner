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
        print(f"Error opening workflow metadata file: {e!r}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding workflow metadata JSON: {e!r}")
        sys.exit(1)


def get_workflow_metadata_from_api(workflow_id: str):
    url = f"{CROMWELL_URL}/api/workflows/v1/{workflow_id}/metadata"
    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {get_cromwell_oauth_token()}',
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching workflow metadata: {e!r}")
        sys.exit(1)


def parse_workflow_status_and_outputs(json_data: dict):
    sg_id = None
    dataset = None
    status = {}
    outputs: dict[str, dict] = {}
    calls = json_data.get('calls', {})

    for key, value in calls.items():
        if key.startswith('GatherSampleEvidence.'):
            workflow_name = key.split('.')[-1]
            if value:
                # Get the dataset and sequencing group ID
                if not dataset or not sg_id:
                    labels = value[0].get('backendLabels', {})
                    dataset = labels.get('dataset')
                    sg_id = labels.get('sequencing-group')

                # Get the execution status
                execution_status = value[0].get('executionStatus')

                # Check for failures
                if 'failures' in value[0]:
                    execution_status = 'Failed'
                    failure_message = value[0]['failures'][0].get(
                        'message',
                        'Unknown failure',
                    )
                    status[workflow_name] = f"{execution_status}: {failure_message}"
                else:
                    status[workflow_name] = execution_status

                # Get the outputs
                outputs[workflow_name] = {}
                if workflow_outputs := value[0].get('outputs', {}):
                    for output_key, output_value in workflow_outputs.items():
                        if output_value.endswith(('cram', 'crai')):
                            continue
                        outputs[workflow_name][output_key] = output_value

            else:
                status[workflow_name] = 'Not Started'

    if not sg_id:
        raise ValueError("SG ID not found in metadata")
    return {sg_id.upper(): {'dataset': dataset, 'status': status, 'outputs': outputs}}


def copy_outputs_to_bucket(
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

    for _, output in outputs.items():
        for _, value in output.items():
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
            if not dry_run:
                print(f'Copying {source_blob.name} to {destination_gs_url}')
                blob_copy = source_bucket.copy_blob(
                    source_blob,
                    destination_bucket,
                    destination_blob_name,
                )
                print(f"Blob {blob_copy.name} copied")
            else:
                print(
                    f"DRY RUN: Would have copied {source_blob.name} to {destination_gs_url}",
                )

    return analysis_file_sizes


def get_analyses_to_create(
    sg_id: str,
    participant_eid: str,
    dataset: str,
    analysis_file_sizes: dict,
):
    """Queues the analyses to be created."""
    analyses = []
    for analysis_type in ['scramble', 'wham', 'manta']:
        if analysis_file_sizes.get(analysis_type) is None:
            print(f'No {analysis_type} outputs found for {sg_id}.')
            continue
        output_path = to_path(
            f'gs://cpg-{dataset}-main/sv_evidence/{sg_id}.{analysis_type}.vcf.gz',
        )
        sv_analysis = Analysis(
            type='sv',
            output=str(output_path),
            meta={
                "stage": "GatherSampleEvidence",
                "sequencing_type": "genome",
                "dataset": dataset,
                "sequencing_group": sg_id,
                "participant_id": participant_eid,
                "size": analysis_file_sizes[analysis_type],
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
        create_sv_analyses(sg_datasets, sg_analyses),
    )


async def create_sv_analyses_async(dataset_sgs: dict, sg_analyses: dict):
    """
    Asynchronously create analyses for the given sequencing groups and datasets.
    """
    aapi = AnalysisApi()
    promises = []
    for dataset, sgs in dataset_sgs.items():
        for sg_id in sgs:
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
    for wf_id in workflow_id:
        json_data = get_workflow_metadata_from_file(wf_id)
        workflow_status = parse_workflow_status_and_outputs(json_data)

        sg_id = next(iter(workflow_status.keys()))
        wf_dataset = workflow_status[sg_id]['dataset']
        sg_datasets[sg_id] = wf_dataset
        status = workflow_status[sg_id]['status']
        outputs = workflow_status[sg_id]['outputs']

        print(f"Workflow Status for ID {workflow_id}:")
        print(f"  Dataset: {wf_dataset}, Sequencing Group ID: {sg_id}")
        for workflow_name, execution_status in status.items():
            print(f"  {workflow_name}: {execution_status}")
        print(f'{len(outputs)} outputs found:')
        for workflow_name, output in outputs.items():
            print(f"  {workflow_name}:")
            for key, value in output.items():
                print(f"    {key}: {value}")

        # Copy outputs to bucket
        source_bucket_name = 'cpg-seqr-main-temp'
        destination_bucket_name = f'cpg-{wf_dataset}-main'
        analysis_file_sizes = copy_outputs_to_bucket(
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
        for sg_id, sg_dataset in sg_datasets.items():
            print(f"Dataset: {sg_dataset}")
            print(
                f"Sequencing Group ID: {sg_id}, Would create: {len(sg_analyses[sg_id])} SV analyses",
            )


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
