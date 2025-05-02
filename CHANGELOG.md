# Changelog

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

* Merge pull request #4 from TomHarrop/parse_metadata_spec.

  Parse metadata spec

* Merge pull request #3 from TomHarrop/data-sanitisation-and-mapping.

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

* Merge branch 'main' into data-sanitisation-and-mapping.

# Conflicts

# .gitignore

# src/atol_bpa_datamapper/config_parser.py

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

* Merge remote-tracking branch 'refs/remotes/origin/main'

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
