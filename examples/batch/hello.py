"""
Test Hail Batch Workflow

    cd examples/batch
    analysis-runner \
      --access-level test \
      --dataset fewgenomes \
      --description "Run Batch" \
      --output-dir "$(whoami)/hello-world" \
      hello.py \
      --name-to-print $(whoami)
"""
from shlex import quote
import hailtop.batch as hb
import click
from cpg_utils.hail_batch import get_config, remote_tmpdir, output_path


@click.command()
@click.option('--name-to-print')
@click.option('--output-suffix', required=False)
def main(name_to_print, output_suffix):
    """Runs test hail batch workflow"""
    config = get_config()

    sb = hb.ServiceBackend(
        billing_project=config['hail']['billing_project'],
        remote_tmpdir=remote_tmpdir(),
    )

    b = hb.Batch(backend=sb)

    j1 = b.new_job('first job')
    # For Hail batch, j.{identifier} will create a Resource (file)
    # that will be collected at the end of a job
    stdout_of_j = j1.out
    string_to_print = f'Hello, {name_to_print}'
    j1.command(f'echo {quote(string_to_print)} > {stdout_of_j}')

    j2 = b.new_job('second job')
    # for the second job, using an f-string with the resource file
    # will tell batch to run j2 AFTER j1
    j2.command(f'cat {stdout_of_j}')

    b.write_output(stdout_of_j, output_path(output_suffix))

    # use wait=False, otherwise this line will hang while the sub-batch runs
    # bad for running hail batch within a hail batch, as preemption
    b.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
