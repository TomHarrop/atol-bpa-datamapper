# Changelog

## (unreleased)

### Changes

* Remove organism_grouping_key.

## v0.3.1 (2026-03-06)

### Other

* .gitignore .vscode files.

* Add organism_grouping_key to merged specimens and log specimens in transform_data summary.

## v0.3.0 (2026-02-27)

### Fix

* Check taxon_id is valid.

## v0.2.0 (2026-02-13)

### Other

* Add NA specimen strings to config.

* Add condense code to read_schema script, should probably split.

* Update config and test workflow.

## v0.1.17 (2025-12-12)

### Changes

* Get the order and family from the NCBI taxonomy.

## v0.1.16 (2025-12-09)

### Changes

* Argument format for transform-data.

## v0.1.15 (2025-12-09)

### Changes

* Move augustus dataset mapping to config, since it's shipped anyway.

## v0.1.14 (2025-12-08)

### New

* Retrieve common name, authority and lineage string during mapping.

### Other

* Readme notes.

* Flatten mapped jsonl to csv for testing.

## v0.1.13 (2025-10-17)

### Changes

* Prune the NCBI tree to the Augustus datasets, and search for closest.

* Handle boolean optional_file field.

* Remove underscores from organism_grouping_key (fixes #31)

### Other

* Update taxon names and new species.

* Testing.

* Create taxid_to_AUGUSTUSdataset.tsv.

## v0.1.12 (2025-09-11)

### Changes

* Library_type defaults to None (fixes #29)

* Default for library_selection (addresses #29)

### Other

* Remove duplicate logic in test, remove redundant factory fixture.

* Add tests to improve coverage.

* Remove redundant fixtures.

* Undo temporary code accidentally committed.

* Make sanitization config file an arg, update tests.

* Make sanitization config file an arg, update tests.

* Update key for unique samples in tests.

* Add assertion to test default fields.

* Remove my incorrect fixture, also remove unit tests which are already covered by integration tests.

* Update tests.

* Fix failing config parser integration test.

* Updating unit tests.

* Passing with noted issues wrt counters for any nested values (to do!)

* Add unit and integration tests for counters in filter_packages.

* Add unit and integration tests for counters in filter_packages.

* Add unit and integration tests for counters in filter_packages.

* Update unit tests for config_parser to test default values.

* Add tests for transform_data.

* Update contest.py and gitignore.

* Format experiments data for database.

* Update tests for package handler.

* Add factory fixtures for taxdump, metadata_map and args handler.

* Update unit and integration tests.

* Update unit and integration tests.

## v0.1.11 (2025-08-06)

### Other

* Only use NCBI taxid and scientific_name for grouping key (fixes #26)

* Only use NCBI taxid and scientific_name for grouping key (fixes #26)

## v0.1.10 (2025-07-29)

### Other

* Update README.md and code to match updated schema.

* Update README.md.

* Update io.py.

* Update vocab.

* Sync config files with AToL schema spreadsheets.

* Add recursive safe_get function.

* Update README.md with the transform-data args.

* Add organism_grouping_key to unique samples output.

* Abstract out a base EntityTransformer class to handle overlapping logic in OrganismTransformer and SampleTransformer.

* Revert an accidental code change.

* Update transform-data arg names for consistency.

* Ignore any inconsistencies for specified fields between the same representations of the same entities (for both organisms and samples)

* Add logic to extract unique organisms and log conflicts.

* Update config (to be updated in source spreadsheet)

* Update README.md.

* Change package_map format.

* Fix recording transformation changes.

* Update conflict dict to be more readable.

* Ignore conflicting samples from output and log; if conflict is on sample_access_date, take most recent date to both preserve privacy and prevent throwing out usable samples.

* Make transform_data an executable script in pyproject.toml, update io.py to handle input of the format matching the output from map-metadata, add output file to track changes during transform_data step.

* Added some auto-generated code comments, have edited comments for accuracy and brevity.

* Added some auto-generated code comments, have edited comments for accuracy and brevity.

* Extract samples with the same sample_name, detect and report any conflicts between metadata fields for samples with the same sample_name attribute, report bpa_package_id where each sample is found.

## v0.1.9 (2025-07-04)

### New

* Update the taxon_id if a single match is found in the taxonomy.

## v0.1.8 (2025-07-03)

### New

* Analyse the decision_log to find values that need mapping.

* Vocab for runs.

* Vocabularies for mandatory Sample and Organism fields.

### Changes

* Start mapping BPA terms to SRA terms.

* If there is a default, non-accepted values are kept and mapped to default (fixes #21)

* Check null strings in choose_value (fixes #20)

* Case-insensitive vocab matching.

* Sanitise sample.lifestage.

### Other

* Add some terms from the decision log.

* Experiment vocab.

## v0.1.6 (2025-06-06)

### New

* Group Packages by organism during mapping.

* Include the BPA id in the output.

### Changes

* Use null values from config for grouping"

## v0.1.5 (2025-06-02)

### Changes

* Add sex to sample section.

* Map missing values to None during sanitisation.

## v0.1.4 (2025-05-26)

### Changes

* Fall back to parent object if value not in current object (fixes #9)

* Map metadata for Resources and add to Package output as an array of sections.

* Map Resource metadata.

* Find all values from nested resources in get_nested_value (fixes #6)

* Filter Packages and Resources in filter_packages.py.

* Filter resources while iterating through packages. chg: lookup fields in the parent package when running choose_value on resources.

* Fork the filtering methods.

* Filter_packages is only filtering on Package-level metadata Report KeyError in MetadataMap via logger (fixes #1)

* Package filtering on package fields only.

* Read split metadata schema.

* Parse package metadata schema separately.

### Other

* Run manual tests.

* Mapping only at resource level.

* Generate a dict of resources for each package.

## v0.1.3 (2025-05-02)

### Changes

* Parse controlled vocab.

* Experiment is package-level.

* Handle resource sections that aren't called runs.

* Handle resource sections that aren't called runs.

* Parse google sheets config.

* Readme.

* Readme.

* Update test script.

* Update schema mapping script.

* Parse AToL schema to mapping config.

### Other

* Update README.md.

* Ignore resource-level objects during filtering, handle during mapping step only.

* Update logic to filter resource-level fields.

* Refactor choose_value function to remove duplication, update tests.

* Add descriptions to tests.

* Update tests.

* Update tests.

* Update tests.

* Update tests.

* Update tests.

* Update tests.

* Update schema to match ENA submission, refactor logic for handling resources.

* Currently we are accepting empty strings as a value "found", but there might be more meaningful values lower in the list of BPA field options. This commit passes over fields with empty strings, and also adds a relevant test case.

* Resolve failing unit test for read objects with possible mappings to parent objects.

* Add test case for read fields mapping to parent object properties.

* Save keep attribute.

* Add unit tests for mapping nested fields.

* Mapping tests nested fields.

* Fix filter method for nested resources, add test cases.

* Remove duplicate call to sanitize values.

* Improve sanitization.

* Add bpa_id to transformed data, add sanitization rules applied after field mapping and before value mapping.

* Add bpa_id to transformed data, add sanitization rules applied after field mapping and before value mappi g.

* Add unit and integration tests for config parser and package handler.

* Add unit and integration tests for config parser and package handler.

* Fix multiple resource mapping.

* Update .gitignore.

* Handle multiple resources per package, mapping to multiple reads in mapped output.

* Read schema direct from google sheet.

* Update README.md.

## v0.1.2 (2025-03-13)

### Other

* Print groups to output.

* Rudimentary package sorting.

* Remove profiling.

* Abandon multiprocessing, IPC is too slow.

* Attempt multiprocessing.

* Handle ranks.

* Traverse NCBI taxonomy tree for ranks.

* Lookup taxid in taxonomy.

* Start looking for species information in the metadat.

* Check accession against ncbi.

* Basic taxdump lookup.

* Read and cache the NCBI taxonomy.

## v0.1.1 (2025-02-24)

### New

* Initial release.

## v0.1.0 (2025-02-24)

### Other

* Cosmetic: changelog.

* Add counts for metadata mapping.

* Test access rights.

* Start logging mapping decision.

* Genome? yes!

* Filtering ok.

* Logging.

* Filtering ok.

* Logging.

* Start mapping.

* Getting default filtering config.

* Config.

* Move.

* Move.

* Move.

* Move.

* Move.

* Toml.

* Toml.

* Start mapping.

* Toml.

* Toml.

* Toml.

* Toml.

* IO.

* Initial IO code.

* Initial commit.
