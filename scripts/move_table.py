"""
copies a hail table from one location to another
"""
import click

import hail as hl

from cpg_utils.hail_batch import init_batch


@click.command()
@click.option('--input_path')
@click.option('--output_path')
def main(input_path: str, output_path: str):
    """
    move a hail table from one location to another
    """
    init_batch()
    ht = hl.read_table(input_path)
    ht.write(output_path)


if __name__ == '__main__':
    main()
