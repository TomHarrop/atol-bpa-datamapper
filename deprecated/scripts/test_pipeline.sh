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

python3 -m cProfile -o "results/${RESULT_DIR}/filter.pipeline.stats" \
    -m atol_bpa_datamapper.pipeline \
    --nodes "dev/taxdump/nodes.dmp" \
    --names "dev/taxdump/names.dmp" \
    --taxids_to_busco_dataset_mapping "dev/mapping_taxids-busco_dataset_name.eukaryota_odb10.2019-12-16.txt.tar.gz" \
    --decision_log "results/${RESULT_DIR}/filter/decision_log.csv.gz" \
    --sample-conflicts "results/${RESULT_DIR}/transform/SAMPLE_CONFLICTS.jsonl.gz" \
    --unique-organisms "results/${RESULT_DIR}/transform/UNIQUE_ORGANISMS" \
    <"${INPUT_DATA}" \
    >"results/${RESULT_DIR}/f.jsonl.gz"

exit 0

Rscript dev/scripts/analyse_decision_log.R "${RESULT_DIR}/filter" &

python3 -m cProfile -o "results/${RESULT_DIR}/map.cprofile.stats" \
    -m atol_bpa_datamapper.map_metadata \

    --raw_field_usage "results/${RESULT_DIR}/map/raw_field_usage_mapping.jsonl.gz" \
    --raw_value_usage "results/${RESULT_DIR}/map/raw_value_usage.jsonl.gz" \
    --mapped_field_usage "results/${RESULT_DIR}/map/mapped_field_usage.jsonl.gz" \
    --mapped_value_usage "results/${RESULT_DIR}/map/mapped_value_usage.jsonl.gz" \
    --unused_field_counts "results/${RESULT_DIR}/map/unused_field_counts.jsonl.gz" \

    <"results/${RESULT_DIR}/f.jsonl.gz" \
    >"results/${RESULT_DIR}/m.jsonl.gz"

python3 -m cProfile -o "results/${RESULT_DIR}/transform.cprofile.stats" \
    -m atol_bpa_datamapper.transform_data \
    --sample-ignored-fields "results/${RESULT_DIR}/transform/SAMPLE_IGNORED_FIELDS" \
    --organism-ignored-fields "results/${RESULT_DIR}/transform/ORGANISM_IGNORED_FIELDS" \
    <"results/${RESULT_DIR}/m.jsonl.gz" \
    >"results/${RESULT_DIR}/t.jsonl.gz"

wait
