#!/usr/bin/env python3

"""
Takes a path to a MatrixTable and a name prefix
Writes data into the test bucket for the dataset

Optionally sample IDs and a locus can be provided to reduce the output
If sample IDs are specified, output subset contains only those
--format arg sets the output type (mt|vcf|both), default is MT
Optionally supply both --chr and --pos; subset to a specific locus
The pos format can be a single int, or a "start-end"
Default behaviour is to remove any sites where the requested samples
    are all HomRef. All sites can be retained with --keep_ref
"""

from argparse import ArgumentParser
import logging
import sys

import hail as hl

from cpg_utils.hail_batch import output_path, init_batch


def subset_to_samples(
    mt: hl.MatrixTable, samples: set[str], keep_hom_ref: bool
) -> hl.MatrixTable:
    """
    checks the requested sample subset exists in this joint call
    reduces the MatrixTable to a sample subset

    Parameters
    ----------
    mt : the current MatrixTable object
    samples : a set of samples requested for the subset selection
    keep_hom_ref : if False, remove sites where no remaining samples have alt calls

    Returns
    -------
    The original MT reduced to only the selected samples, and depending on the
    keep_hom_ref flag, only sites where that sample subset contain alt calls
    """

    missing_samples = samples.difference(set(mt.s.collect()))

    if missing_samples:
        raise AssertionError(
            f'Sample(s) missing from subset: {", ".join(missing_samples)}'
        )

    mt = mt.filter_cols(hl.set(samples).contains(mt.s))

    # optional - filter to variants with at least one alt call in these samples
    if not keep_hom_ref:
        mt = hl.variant_qc(mt)
        mt = mt.filter_rows(mt.variant_qc.n_non_ref > 0)
        mt = mt.drop('variant_qc')

    return mt


def subset_to_locus(mt: hl.MatrixTable, locus: hl.IntervalExpression) -> hl.MatrixTable:
    """
    Subset the provided MT to a single locus - fail if the variant is absent

    Parameters
    ----------
    mt : the current MatrixTable object
    locus : a hail LocusExpression indicating the range of positions to select

    Returns
    -------
    The subset of the MatrixTable overlapping the indicated locus
    """

    mt = mt.filter_rows(locus.contains(mt.locus))
    if mt.count_rows() == 0:
        raise ValueError(f'No rows remain after applying Locus filter {locus}')
    return mt


def main(
    mt_path: str,
    prefix: str,
    samples: set[str],
    out_format: str,
    locus: hl.IntervalExpression | None,
    all_ref: bool,
    sample_ref: bool,
    biallelic: bool = False,
):
    """

    Parameters
    ----------
    mt_path : path to input MatrixTable
    prefix : prefix for file naming
    samples : a set of samples to reduce the joint-call to
    out_format : whether to write as a MT, VCF, or Both
    locus : an optional parsed interval for locus-based selection
    all_ref : if true, retain sites without alt calls in the subset
    sample_ref : if true, retain called reference sites
    biallelic : if True, filter the output MT to biallelic sites only
    """

    mt = hl.read_matrix_table(mt_path)

    if samples:
        mt = subset_to_samples(mt, samples=samples, keep_hom_ref=sample_ref)

    if isinstance(locus, hl.IntervalExpression):
        mt = subset_to_locus(mt, locus=locus)

    # biallelic flag reduces to [ref, alt] rows
    if biallelic:
        mt = mt.filter_rows(hl.len(mt.alleles) == 2)

    # base assumption is that we want to remove reference blocks (monoallelic)
    # this filter permits multi-allelics
    elif not all_ref:
        mt = mt.filter_rows(hl.len(mt.alleles) >= 2)

    # write data to test output paths
    if out_format in ['mt', 'both']:
        matrixtable_path = output_path(f'{prefix}.mt', test=True)
        mt.write(matrixtable_path, overwrite=True)
        logging.info(f'Wrote new MT to {matrixtable_path!r}')

    # if VCF, export as a VCF as well
    if out_format in ['vcf', 'both']:
        vcf_path = output_path(f'{prefix}.vcf.bgz', test=True)
        hl.export_vcf(mt, vcf_path, tabix=True)
        logging.info(f'Wrote new table to {vcf_path!r}')

    if out_format == 'ht':
        table_path = f'{output_path}.ht'
        mt.rows().write(table_path, overwrite=True)
        logging.info(f'Wrote new HT to {table_path!r}')


def clean_locus(contig: str, pos: str) -> hl.IntervalExpression | None:
    """

    Parameters
    ----------
    contig : the contig string, e.g. 'chr4'
    pos : either a single coordinate or two in the form start-stop

    Returns
    -------
    A parsed hail locus. For a point change this will be contig:pos-pos+1
    """
    if not any([contig, pos]):
        return None

    if pos and not contig:
        raise AssertionError(f'Positional filtering requires a chromosome')

    if contig and not pos:
        start = 'start'
        end = 'end'

    elif '-' in pos:
        assert (
            pos.count('-') == 1
        ), f'Positions must be one value, or a range between two values: {pos}'
        start, end = pos.split('-')
        if start != 'start':
            assert int(start), f'start value could not be converted to an int: {start}'
            if int(start) < 1:
                start = 1
        if end != 'end':
            assert int(end), f'end value could not be converted to an int: {end}'
            # adjust the end value if it is out of bounds
            if int(end) > hl.get_reference('GRCh38').lengths[contig]:
                end = hl.get_reference('GRCh38').lengths[contig]

        # final check that numeric coordinates are ordered
        if start != 'start' and end != 'end':
            assert int(start) < int(end)

    else:
        assert int(
            pos
        ), f'if only one position is specified, it must be numerical: {pos}'
        start = int(pos)
        end = start + 1

    return hl.parse_locus_interval(f'{contig}:{start}-{end}', reference_genome='GRCh38')


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
        help='Prefix for MT/VCF name\n'
        '("output" will become output.vcf.bgz or output.mt)',
        required=True,
    )
    parser.add_argument(
        '-s', help='One or more sample IDs, whitespace delimited', nargs='+', default=[]
    )
    parser.add_argument(
        '--format',
        help='Write output in this format. "both" writes MT and VCF, "HT" writes the '
        'rows from a MatrixTable (no genotypes)',
        default='mt',
        choices=['both', 'mt', 'vcf', 'ht'],
    )
    parser.add_argument('--chr', help='Contig portion of a locus', required=False)
    parser.add_argument(
        '--pos',
        help='Pos portion of a locus. Can be "12345" or "12345-67890" for a range',
        required=False,
    )
    parser.add_argument(
        '--keep_all_ref',
        help='Output will retain sites which represent reference calls; e.g. if input '
        'MT has not been densified, this flag will cause the output to contain '
        'rows with a single allele, representing a reference call or block. This '
        'argument conflicts with --biallelic',
        action='store_true',
    )
    parser.add_argument(
        '--keep_sample_ref',
        help='Output will retain sites where the sample subset is HomRef; e.g. if '
        'you subset to a group of samples such that no remaining sample has an '
        'alt call, the default behaviour is to drop those calls',
        action='store_true',
    )
    parser.add_argument(
        '--biallelic',
        help='Remove non-biallelic sites. This argument conflicts with --keep_all_ref',
        action='store_true',
    )
    args, unknown = parser.parse_known_args()

    if unknown:
        raise ValueError(f'Unknown args, could not parse: {unknown!r}')

    assert not (
        args.biallelic and args.keep_all_ref
    ), 'choose one of --biallelic and --keep_all_ref'

    init_batch()
    locus_interval = clean_locus(args.chr, args.pos)

    main(
        mt_path=args.i,
        prefix=args.out,
        samples=set(args.s) if args.s else None,
        out_format=args.format,
        locus=locus_interval,
        all_ref=args.keep_all_ref,
        sample_ref=args.keep_sample_ref,
        biallelic=args.biallelic,
    )
