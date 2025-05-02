set -eux

INPUT_DATA="dev/datasets.jsonl.gz"
RESULT_DIR="$(date -I)_${RANDOM}"

[ -d "results/${RESULT_DIR}" ] && exit 1 || mkdir -p "results/${RESULT_DIR}"

filter-packages \
    --raw_field_usage "results/${RESULT_DIR}/raw_field_usage_filtering.jsonl.gz" \
    --bpa_field_usage results/${RESULT_DIR}/bpa_field_usage.jsonl.gz \
    --bpa_value_usage "results/${RESULT_DIR}/bpa_value_usage.jsonl.gz" \
    --decision_log "results/${RESULT_DIR}/decision_log.csv.gz" \
    <"${INPUT_DATA}" \
    >"results/${RESULT_DIR}/f.jsonl.gz"

map-metadata \
    --raw_field_usage "results/${RESULT_DIR}/raw_field_usage_mapping.jsonl.gz" \
    --raw_value_usage "results/${RESULT_DIR}/raw_value_usage.jsonl.gz" \
    --mapped_field_usage "results/${RESULT_DIR}/mapped_field_usage.jsonl.gz" \
    --mapped_value_usage "results/${RESULT_DIR}/mapped_value_usage.jsonl.gz" \
    --unused_field_counts "results/${RESULT_DIR}/unused_field_counts.jsonl.gz" \
    --mapping_log "results/${RESULT_DIR}/decision_log.csv.gz" \
    <"results/${RESULT_DIR}/f.jsonl.gz" \
    >"results/${RESULT_DIR}/m.jsonl.gz"
