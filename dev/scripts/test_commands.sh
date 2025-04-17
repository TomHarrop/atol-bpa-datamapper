filter-packages \
    --raw_field_usage results/2025-04-17/raw_field_usage_filtering.jsonl.gz \
    --bpa_field_usage results/2025-04-17/bpa_field_usage.jsonl.gz \
    --bpa_value_usage results/2025-04-17/bpa_value_usage.jsonl.gz \
    --decision_log results/2025-04-17/decision_log.csv.gz \
    <dev/datasets.jsonl.gz \
    >results/2025-04-17/f.jsonl.gz

map-metadata \
    --raw_field_usage results/2025-04-17/raw_field_usage_mapping.jsonl.gz \
    --raw_value_usage results/2025-04-17/raw_value_usage.jsonl.gz \
    --mapped_field_usage results/2025-04-17/mapped_field_usage.jsonl.gz \
    --mapped_value_usage results/2025-04-17/mapped_value_usage.jsonl.gz \
    --unused_field_counts results/2025-04-17/unused_field_counts.jsonl.gz \
    --mapping_log results/2025-04-17/decision_log.csv.gz \
    <results/2025-04-17/f.jsonl.gz \
    >results/2025-04-17/m.jsonl.gz
