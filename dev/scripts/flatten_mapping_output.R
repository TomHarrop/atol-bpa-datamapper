#!/usr/bin/env Rscript

library(data.table)
library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  results_dir <- "2025-10-17_14857"
} else {
  results_dir <- args[1]
}

mapped_metadata_file <- paste(c("results", results_dir, "m.jsonl.gz"),
  collapse = "/"
)
mapped_packages_csv <- paste(c("results", results_dir, "map", "m.packages.csv.gz"),
  collapse = "/"
)
mapped_resources_csv <- paste(c("results", results_dir, "map", "m.resources.csv.gz"),
  collapse = "/"
)


mapped_metadata_dt <- as.data.table(
  stream_in(
    file(
      mapped_metadata_file
    ),
    flatten = TRUE
  )
)

runs <- mapped_metadata_dt[, runs]
names(runs) <- mapped_metadata_dt[, experiment.bpa_package_id]
runs_dt <- rbindlist(runs, idcol = "experiment.bpa_package_id", fill = TRUE)


fwrite(mapped_metadata_dt[, !c("runs")], mapped_packages_csv)
fwrite(runs_dt, mapped_resources_csv)
