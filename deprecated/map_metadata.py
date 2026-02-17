#!/usr/bin/env python3

from .organism_mapper import NcbiTaxdump


def main():

    # set up taxonomy data
    ncbi_taxdump = NcbiTaxdump(
        nodes_file=args.nodes,
        names_file=args.names,
        taxids_to_busco_dataset_mapping=args.taxids_to_busco_dataset_mapping,
        taxids_to_augustus_dataset_mapping=args.taxids_to_augustus_dataset_mapping,
        cache_dir=args.cache_dir,
        resolve_to_rank="species",
    )

    if not args.dry_run:
        if args.grouping_log:
            logger.info(f"Writing grouping log to {args.grouping_log}")
            write_mapping_log_to_csv(grouping_log, args.grouping_log)
