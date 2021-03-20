"""Simple Hail query example."""

import click
import hail as hl
from bokeh.io.export import get_screenshot_as_png


GNOMAD_HGDP_1KG_MT = (
    'gs://gcp-public-data--gnomad/release/3.1/mt/genomes/'
    'gnomad.genomes.v3.1.hgdp_1kg_subset_dense.mt'
)


@click.command()
@click.option('--output', help='GCS output path', required=True)
@click.option('--rerun', help='Whether to overwrite cached files', default=False)
def query(output, rerun):
    """Query script entry point."""

    hl.init(default_reference='GRCh38')

    sample_qc_path = f'{output}/sample_qc.mt'
    if rerun or not hl.hadoop_exists(sample_qc_path):
        mt = hl.read_matrix_table(GNOMAD_HGDP_1KG_MT)
        mt = mt.head(100, n_cols=100)
        mt_qc = hl.sample_qc(mt)
        mt_qc.write(sample_qc_path)
    mt_qc = hl.read_matrix_table(sample_qc_path)

    plot_filename = f'{output}/call_rate_plot.png'
    if rerun or not hl.hadoop_exists(plot_filename):
        call_rate_plot = hl.plot.histogram(
            mt_qc.sample_qc.call_rate, range=(0, 1), legend='Call Rate'
        )
        with hl.hadoop_open(plot_filename, 'w') as f:
            get_screenshot_as_png(call_rate_plot).save(f)


if __name__ == '__main__':
    query()  # pylint: disable=no-value-for-parameter
