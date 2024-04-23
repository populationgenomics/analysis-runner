"""Simple Hail query example."""

import click
from bokeh.io.export import get_screenshot_as_png

import hail as hl

from cpg_utils.config import output_path

GNOMAD_HGDP_1KG_MT = (
    'gs://gcp-public-data--gnomad/release/3.1/mt/genomes/'
    'gnomad.genomes.v3.1.hgdp_1kg_subset_dense.mt'
)


@click.command()
@click.option(
    '--rerun',
    help='Whether to overwrite cached files',
    is_flag=True,
    default=False,
)
def query(rerun: bool):
    """Query script entry point."""

    hl.init(default_reference='GRCh38')

    sample_qc_path = output_path('sample_qc.mt')
    if rerun or not hl.hadoop_exists(sample_qc_path):
        mt = hl.read_matrix_table(GNOMAD_HGDP_1KG_MT)
        mt = mt.head(100, n_cols=100)
        mt_qc = hl.sample_qc(mt)
        mt_qc.write(sample_qc_path)
    mt_qc = hl.read_matrix_table(sample_qc_path)

    plot_filename = output_path('call_rate_plot.png', 'web')
    if rerun or not hl.hadoop_exists(plot_filename):
        call_rate_plot = hl.plot.histogram(
            mt_qc.sample_qc.call_rate,
            range=(0, 1),
            legend='Call rate',
        )
        with hl.hadoop_open(plot_filename, 'wb') as f:
            get_screenshot_as_png(call_rate_plot).save(f, format='PNG')


if __name__ == '__main__':
    query()
