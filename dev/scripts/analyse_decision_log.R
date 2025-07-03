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

GetSingleFailures <- function(x, decision_log, single_fails) {
  varname <- sub("_accepted", "", x)
  single_rejection_counts <- decision_log[
    id %in% single_fails & x == FALSE, .N,
    by = varname, env = list(x = x, varname = varname)
  ][order(N, decreasing = TRUE)]

  setnames(single_rejection_counts, varname, "value")
  single_rejection_counts[, field := varname]
  setcolorder(single_rejection_counts, c("field", "value", "N"))
  return(single_rejection_counts)
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  results_dir <- "2025-07-02_15210"
}
results_dir <- args[1]


decision_log_file <- paste(c("results", results_dir, "decision_log.csv.gz"),
  collapse = "/"
)
failed_counts_file <- paste(c("results", results_dir, "failed_counts.csv"),
  collapse = "/"
)
single_fails_file <- paste(c("results", results_dir, "single_fail_values.csv"),
  collapse = "/"
)



decision_log <- fread(decision_log_file)

col_classes <- decision_log[, sapply(.SD, class)]
lgc_cols <- names(col_classes[col_classes == "logical"])

sdc <- lgc_cols[lgc_cols != "kept_resources"]


# get an ordered list of everything failing filtering
failed_counts <- rbindlist(
  lapply(sdc, GetRejectedValues, decision_log = decision_log)
)
fwrite(failed_counts, failed_counts_file)


# check for datasets that only failed on one field
setkey(decision_log, id)
fails_per_sample <- decision_log[,
  .(n_fails = sum(!.SD)),
  .SDcols = lgc_cols, by = id
]
single_fails <- fails_per_sample[n_fails == 1, unique(id)]


single_fail_values <- rbindlist(
  lapply(
    lgc_cols,
    GetSingleFailures,
    decision_log = decision_log, single_fails = single_fails
  )
)
fwrite(single_fail_values, single_fails_file)
