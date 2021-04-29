"""Simple Hail query example."""

import hail as hl
from gnomad.utils.sparse_mt import impute_sex_ploidy
from gnomad.resources.grch38 import telomeres_and_centromeres


def query():
    """Query script entry point."""

    hl.init(default_reference='GRCh38')

    mt_path = 'gs://cpg-tob-wgs-temporary/joint_vcf/raw/genomes.mt/'
    mt = hl.read_matrix_table(mt_path)
    ploidy_ht = impute_sex_ploidy(
        mt,
        excluded_calling_intervals=None,
        normalization_contig='chr20',
    )
    ploidy_ht.show()

    ploidy_ht = impute_sex_ploidy(
        mt,
        excluded_calling_intervals=telomeres_and_centromeres.ht(),
        normalization_contig='chr20',
    )
    ploidy_ht.show()

    # sample_qc_path = f'{output}/sample_qc.mt'
    # if rerun or not hl.hadoop_exists(sample_qc_path):
    #     mt = hl.read_matrix_table(GNOMAD_HGDP_1KG_MT)
    #     mt = mt.head(100, n_cols=100)
    #     mt_qc = hl.sample_qc(mt)
    #     mt_qc.write(sample_qc_path)
    # mt_qc = hl.read_matrix_table(sample_qc_path)

    # plot_filename = f'{output}/call_rate_plot.png'
    # if rerun or not hl.hadoop_exists(plot_filename):
    #     call_rate_plot = hl.plot.histogram(
    #         mt_qc.sample_qc.call_rate, range=(0, 1), legend='Call rate'
    #     )
    #     with hl.hadoop_open(plot_filename, 'wb') as f:
    #         get_screenshot_as_png(call_rate_plot).save(f, format='PNG')


if __name__ == '__main__':
    query()  # pylint: disable=no-value-for-parameter
