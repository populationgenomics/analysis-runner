"""Simple Hail query example."""

import hail as hl
import click


GNOMAD_HGDP_1KG_MT = (
    'gs://gcp-public-data--gnomad/release/3.1/mt/genomes/'
    'gnomad.genomes.v3.1.hgdp_1kg_subset_dense.mt'
)


@click.command()
@click.option('--output', help='GCS output path', required=True)
def query(output):
    """Query script entry point."""

    hl.init(default_reference='GRCh38')

    mt = hl.read_matrix_table(GNOMAD_HGDP_1KG_MT)
    mt = mt.head(100, n_cols=100)

    mt = mt.sample_qc()
    mt.write(f'{output}/sample_qc.mt')


if __name__ == '__main__':
    query()  # pylint: disable=no-value-for-parameter
