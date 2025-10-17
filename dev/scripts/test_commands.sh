set -eux

INPUT_DATA="dev/datasets.jsonl.gz"
RESULT_DIR="$(date -I)_${RANDOM}"

[ -d "results/${RESULT_DIR}" ] && exit 1 || mkdir -p "results/${RESULT_DIR}/"{filter,map,transform}

if [ ! -f "${INPUT_DATA}" ]; then
    ckanapi search datasets \
        -a "${BPI_APIKEY}" \
        include_private=true \
        -O "${INPUT_DATA}" \
        -z -r https://data.bioplatforms.com
fi

python3 -m cProfile -o "results/${RESULT_DIR}/filter.cprofile.stats" \
    -m atol_bpa_datamapper.filter_packages \
    --raw_field_usage "results/${RESULT_DIR}/filter/raw_field_usage_filtering.jsonl.gz" \
    --bpa_field_usage results/${RESULT_DIR}/filter/bpa_field_usage.jsonl.gz \
    --bpa_value_usage "results/${RESULT_DIR}/filter/bpa_value_usage.jsonl.gz" \
    --decision_log "results/${RESULT_DIR}/filter/decision_log.csv.gz" \
    <"${INPUT_DATA}" \
    >"results/${RESULT_DIR}/f.jsonl.gz"

Rscript dev/scripts/analyse_decision_log.R "${RESULT_DIR}/filter" &

python3 -m cProfile -o "results/${RESULT_DIR}/map.cprofile.stats" \
    -m atol_bpa_datamapper.map_metadata \
    --nodes "dev/taxdump/nodes.dmp" \
    --names "dev/taxdump/names.dmp" \
    --taxids_to_busco_dataset_mapping "dev/mapping_taxids-busco_dataset_name.eukaryota_odb10.2019-12-16.txt.tar.gz" \
    --taxids_to_augustus_dataset_mapping "dev/resources/taxid_to_augustus_dataset.tsv" \
    --raw_field_usage "results/${RESULT_DIR}/map/raw_field_usage_mapping.jsonl.gz" \
    --raw_value_usage "results/${RESULT_DIR}/map/raw_value_usage.jsonl.gz" \
    --mapped_field_usage "results/${RESULT_DIR}/map/mapped_field_usage.jsonl.gz" \
    --mapped_value_usage "results/${RESULT_DIR}/map/mapped_value_usage.jsonl.gz" \
    --unused_field_counts "results/${RESULT_DIR}/map/unused_field_counts.jsonl.gz" \
    --mapping_log "results/${RESULT_DIR}/map/mapping_log.csv.gz" \
    --grouping_log "results/${RESULT_DIR}/map/grouping_log.csv.gz" \
    --grouped_packages "results/${RESULT_DIR}/map/grouped_packages.jsonl.gz" \
    --sanitization_changes "results/${RESULT_DIR}/map/sanitization_changes.jsonl.gz" \
    <"results/${RESULT_DIR}/f.jsonl.gz" \
    >"results/${RESULT_DIR}/m.jsonl.gz"

Rscript dev/scripts/flatten_mapping_output.R "${RESULT_DIR}" &

python3 -m cProfile -o "results/${RESULT_DIR}/transform.cprofile.stats" \
    -m atol_bpa_datamapper.transform_data \
    --sample-conflicts "results/${RESULT_DIR}/transform/SAMPLE_CONFLICTS.jsonl.gz" \
    --sample-package-map "results/${RESULT_DIR}/transform/SAMPLE_PACKAGE_MAP" \
    --transformation-changes "results/${RESULT_DIR}/transform/TRANSFORMATION_CHANGES" \
    --unique-organisms "results/${RESULT_DIR}/transform/UNIQUE_ORGANISMS" \
    --organism-conflicts "results/${RESULT_DIR}/transform/ORGANISM_CONFLICTS.jsonl.gz" \
    --organism-package-map "results/${RESULT_DIR}/transform/ORGANISM_PACKAGE_MAP" \
    --sample-ignored-fields "results/${RESULT_DIR}/transform/SAMPLE_IGNORED_FIELDS" \
    --organism-ignored-fields "results/${RESULT_DIR}/transform/ORGANISM_IGNORED_FIELDS" \
    <"results/${RESULT_DIR}/m.jsonl.gz" \
    >"results/${RESULT_DIR}/t.jsonl.gz"

wait
