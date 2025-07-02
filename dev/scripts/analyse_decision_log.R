#!/usr/bin/env rscript

library(data.table)

# The purpose of this script is to use the decision log to find terms that
# needed to be added to the vocabulary.
GetRejectedValues <- function(x, decision_log) {
  varname <- sub("_accepted", "", x)

  rejection_counts <- decision_log[x == FALSE,
    .N,
    by = varname,
    env = list(x = x, varname = varname)
  ][
    order(N, decreasing = TRUE)
  ]

  setnames(rejection_counts, varname, "value")
  rejection_counts[, field := varname]
  setcolorder(rejection_counts, c("field", "value", "N"))

  return(rejection_counts)
}


results_dir <- "2025-07-02_10583"

decision_log_file <- paste(c("results", results_dir, "decision_log.csv.gz"),
  collapse = "/"
)
failed_counts_file <- paste(c("results", results_dir, "failed_counts.csv"),
  collapse = "/"
)


decision_log <- fread(decision_log_file)

col_classes <- decision_log[, sapply(.SD, class)]
lgc_cols <- names(col_classes[col_classes == "logical"])

sdc <- lgc_cols[lgc_cols != "kept_resources"]


# get an ordered list of everything failing filtering
failed_counts <- rbindlist(lapply(sdc, GetRejectedValues, decision_log = decision_log))
fwrite(failed_counts, failed_counts_file)




decision_log[, n_passes := sum(.SD), .SDcols = sdc, by = id]

# Packages that only failed one field. The lowest sum indicates the field that
# is causing the most fails.
decision_log[
  n_passes == length(sdc) - 1, lapply(.SD, function(x) sum(!x)),
  .SDcols = sdc
]

decision_log[id == "bpa-tsi-genomics-ddrad-102_100_100_629192-233jlllt3"]

decision_log[n_passes == length(sdc) - 1 & data_context_accepted == FALSE, .N, by = data_context][order(N, decreasing = TRUE)]

decision_log[n_passes == length(sdc) - 1 & library_selection_accepted == FALSE, .N, by = library_selection][order(N, decreasing = TRUE)]

decision_log[n_passes == length(sdc) - 1 & library_strategy_accepted == FALSE, .N, by = library_strategy][order(N, decreasing = TRUE)]

decision_log[n_passes == length(sdc) - 1 & library_source_accepted == FALSE, .N, by = library_source][order(N, decreasing = TRUE)]
