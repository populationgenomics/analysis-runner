#!/usr/bin/env python

"""
Example of a batch script using cpg_pipes.

Note that we disable the pylint import inspection for cpg_pipes. We don't expect
the analysis-runner project to depend on cpg-pipes, which is instead a part of
the Docker image we pull with the run.sh script.
"""
from os.path import join
import click
from cpg_pipes.hailbatch import setup_batch  # pylint: disable=import-error


@click.command()
@click.option('--name')
@click.option('--output-bucket', 'output_bucket')
def main(name: str, output_bucket: str):  # pylint: disable=missing-function-docstring
    b = setup_batch(
        'Test Hail Batch pipeline with cpg_pipes',
        analysis_project_name='fewgenomes',
    )
    j = b.new_job('Test Hail Batch job')
    j.command(f'echo {name} > {j.output}')
    b.write_output(j.output, join(output_bucket, 'result.txt'))
    b.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
