def parse_args_for_mapping():
    input_group.add_argument(
        "--nodes",
        required=True,
        help="NCBI nodes.dmp file from taxdump",
    )

    input_group.add_argument(
        "--names",
        required=True,
        help="NCBI names.dmp file from taxdump",
    )

    input_group.add_argument(
        "--taxids_to_busco_dataset_mapping",
        required=True,
        help="BUSCO placement file from https://busco-data.ezlab.org/v5/data/placement_files/",
    )

    input_group.add_argument(
        "--taxids_to_augustus_dataset_mapping",
        help="File that maps Augustus datasets to NCBI TaxIDs. See config/taxid_to_augustus_dataset.tsv",
        default=get_config_filepath("taxid_to_augustus_dataset.tsv"),
    )

    mapping_group.add_argument(
        "--grouping_log",
        help="Compressed CSV file to record derived organism info for each package",
    )
