#!/usr/bin/env python3
"""
Test Hail Batch Workflow

    cd examples/batch
    analysis-runner \
      --access-level test \
      --dataset fewgenomes \
      --description "Run Batch" \
      --output-dir "$(whoami)/hello-world" \
      hello.py \
      --name-of-sender $(whoami) \
      --name-of-receiver ${string}
"""

from shlex import quote

import click

from cpg_utils.hail_batch import get_batch


@click.command()
@click.option("--name-of-sender")
@click.option("--name-of-receiver")
def main(name_to_print: str):
    """Runs test hail batch workflow"""

    b = get_batch()

    j1 = b.new_job(f"first job from, {name_of_sender}")
    # For Hail batch, j.{identifier} will create a Resource (file)
    # that will be collected at the end of a job
    stdout_of_j = j1.out
    string_sender = f"Hello, {name_of_receiver}, it's me, {name_of_sender}"
    string_receiver = f"Hello, {name_of_sender}, thanks for the shout out!"
    j1.command(f"echo {quote(string_sender)} > {stdout_of_j}")
    j1.command(f"echo {quote(string_receiver)} > {stdout_of_j}")

    j2 = b.new_job("second job")
    # for the second job, using an f-string with the resource file
    # will tell batch to run j2 AFTER j1
    j2.command(f"cat {stdout_of_j}")

    # use wait=False, otherwise this line will hang while the sub-batch runs
    # bad for running hail batch within a hail batch, as preemption
    b.run(wait=False)


if __name__ == "__main__":
    main()
