#!/usr/bin/env python3
# ruff: noqa: PLR2004
"""
Takes a path to a Hail Table and an output name
Writes data into the test bucket for the dataset

Optionally a locus can be provided to reduce the output
Optionally supply both --chr and --pos; subset to a specific locus
The pos format can be a single int, or a "start-end"
"""

import logging
import sys
from argparse import ArgumentParser

import hail as hl

from cpg_utils.hail_batch import init_batch, output_path


def subset_to_locus(ht: hl.Table, locus: hl.IntervalExpression) -> hl.Table:
    """
    Subset the provided Table to a locus

    Parameters
    ----------
    ht : the current Table
    locus : a hail LocusExpression indicating the range of positions to select

    Returns
    -------
    The subset of the Table overlapping the indicated locus
    """

    ht = ht.filter(locus.contains(ht.locus))
    if ht.count() == 0:
        raise ValueError(f'No rows remain after applying Locus filter {locus}')
    return ht


def main(
    ht_path: str,
    output_root: str,
    locus: hl.IntervalExpression | None,
    out_format: str,
    biallelic: bool = False,
):
    """

    Parameters
    ----------
    ht_path : path to input Table
    output_root : prefix for file naming
    locus : an optional parsed interval for locus-based selection
    out_format : the format(s) to write in - ht, vcf, both (default 'ht')
    biallelic : if True, filter the output MT to biallelic sites only
    """

    ht = hl.read_table(ht_path)

    if isinstance(locus, hl.IntervalExpression):
        ht = subset_to_locus(ht, locus=locus)

    if biallelic:
        ht = ht.filter(hl.len(ht.alleles) == 2)

    # just in case
    output_root = output_root.removesuffix('.ht')

    # write data to a test output path
    if out_format in ['ht', 'both']:
        table_path = output_path(f'{output_root}.ht', test=True)
        ht.write(table_path, overwrite=True)
        logging.info(f'Wrote new table to {table_path!r}')

    # if VCF, export as a VCF as well
    if out_format in ['vcf', 'both']:
        vcf_path = output_path(f'{output_root}.vcf.bgz', test=True)
        hl.export_vcf(ht, vcf_path, tabix=True)
        logging.info(f'Wrote new table to {vcf_path!r}')


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
        raise ValueError('Positional filtering requires a chromosome')

    start: int | str
    end: int | str

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
            pos,
        ), f'if only one position is specified, it must be numerical: {pos}'
        start = int(pos)
        end = int(pos) + 1

    return hl.parse_locus_interval(f'{contig}:{start}-{end}', reference_genome='GRCh38')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    parser = ArgumentParser()
    parser.add_argument('-i', help='Path to the input HailTable', required=True)
    parser.add_argument(
        '--out',
        help='output name',
        required=True,
    )
    parser.add_argument('--chr', help='Contig portion of a locus', required=False)
    parser.add_argument(
        '--pos',
        help='Pos portion of a locus. Can be "12345" or "12345-67890" for a range. '
        'Start and end values can be the strings "start" and "end"',
        required=False,
    )
    parser.add_argument(
        '--biallelic',
        help='Remove non-biallelic sites',
        action='store_true',
    )
    parser.add_argument(
        '--format',
        help='Write output in this format',
        default='ht',
        choices=['both', 'ht', 'vcf'],
    )
    args, unknown = parser.parse_known_args()

    if unknown:
        raise ValueError(f'Unknown args, could not parse: "{unknown}"')

    init_batch()
    locus_interval = clean_locus(args.chr, args.pos)

    main(
        ht_path=args.i,
        output_root=args.out,
        locus=locus_interval,
        biallelic=args.biallelic,
        out_format=args.format,
    )
