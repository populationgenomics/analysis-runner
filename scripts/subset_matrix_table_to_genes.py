#!/usr/bin/env python3
# ruff: noqa: PLR2004

"""
Takes a path to a MatrixTable and a name prefix
Writes data into the test bucket for the dataset

Optionally sample IDs and a locus can be provided to reduce the output
If sample IDs are specified, output subset contains only those
--format arg sets the output type (mt|vcf|both), default is MT

new behaviour: `--genes` as a CLI argument, a list of whitespace-delimited ENSG IDs. Filters MT to rows/variants which
    have at least one of the query genes present in the consequence annotations
"""

import logging
import sys
from argparse import ArgumentParser

import hail as hl

from cpg_utils.hail_batch import init_batch


def subset_to_samples(
    mt: hl.MatrixTable,
    samples: set[str],
) -> hl.MatrixTable:
    """
    checks the requested sample subset exists in this joint call
    reduces the MatrixTable to a sample subset

    Parameters
    ----------
    mt : the current MatrixTable object
    samples : a set of samples requested for the subset selection

    Returns
    -------
    The original MT reduced to only the selected samples, and depending on the
    keep_hom_ref flag, only sites where that sample subset contain alt calls
    """

    missing_samples = samples.difference(set(mt.s.collect()))

    if missing_samples:
        raise AssertionError(
            f'Sample(s) missing from subset: {", ".join(missing_samples)}',
        )

    return mt.filter_cols(hl.set(samples).contains(mt.s))


def filter_to_gene_ids(
    mt: hl.MatrixTable,
    gene_ids: set[str],
) -> hl.MatrixTable:
    # turn the python set of Strings into a Hail Set Expression
    hl_gene_set = hl.set(gene_ids)

    # return rows where at least one of the query gene IDs is in the row annotation
    return mt.filter_rows(hl.len(hl_gene_set.intersection(mt.geneIds)) > 0)


def main(
    mt_path: str,
    prefix: str,
    samples: set[str],
    out_format: str,
    genes: set[str] | None = None,
):
    """

    Parameters
    ----------
    mt_path : path to input MatrixTable
    prefix : prefix for file naming
    samples : a set of samples to reduce the joint-call to
    out_format : whether to write as a MT, VCF, or Both
    genes : optional, set of Ensembl ENSG gene IDs
    """

    mt = hl.read_matrix_table(mt_path)

    if samples:
        mt = subset_to_samples(mt, samples=samples)

    if genes:
        mt = filter_to_gene_ids(mt, genes)

    # write data to test output paths
    if out_format in ['mt', 'both']:
        matrixtable_path = f'{prefix}.mt'
        mt.write(matrixtable_path, overwrite=True)
        logging.info(f'Wrote new MT to {matrixtable_path!r}')

    # if VCF, export as a VCF as well
    if out_format in ['vcf', 'both']:
        # remove GVCF INFO field if present - can't be exported to VCF
        if 'gvcf_info' in mt.entry:
            mt = mt.drop('gvcf_info')

        vcf_path = f'{prefix}.vcf.bgz'
        hl.export_vcf(mt, vcf_path, tabix=True)
        logging.info(f'Wrote new table to {vcf_path!r}')

    if out_format == 'ht':
        table_path = f'{prefix}.ht'
        mt.rows().write(table_path, overwrite=True)
        logging.info(f'Wrote new HT to {table_path!r}')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    parser = ArgumentParser()
    parser.add_argument('-i', help='Path to the input MatrixTable', required=True)
    parser.add_argument(
        '--out',
        help='Full prefix for MT/VCF name\n'
        '("output" will become output.vcf.bgz or output.mt)',
        required=True,
    )
    parser.add_argument(
        '-s',
        help='One or more sample IDs, whitespace delimited',
        nargs='+',
        default=[],
    )
    parser.add_argument(
        '--genes',
        help='One or more Ensembl ENSG gene IDs, whitespace delimited',
        nargs='+',
        default=[],
    )
    parser.add_argument(
        '--format',
        help='Write output in this format. "both" writes MT and VCF, "HT" writes the rows from a MatrixTable',
        default='mt',
        choices=['both', 'mt', 'vcf', 'ht'],
    )
    args, unknown = parser.parse_known_args()

    if unknown:
        raise ValueError(f'Unknown args, could not parse: {unknown!r}')

    assert not (
        args.biallelic and args.keep_all_ref
    ), 'choose one of --biallelic and --keep_all_ref'

    init_batch()

    main(
        mt_path=args.i,
        prefix=args.out,
        samples=set(args.s) if args.s else None,
        out_format=args.format,
        genes=args.genes,
    )
