#!/usr/bin/env Rscript

# Script to sync data files
# See https://ecohealthalliance.github.io/eha-ma-handbook/11-cloud-computing-services.html
# For credentials setup

wahis:::push_aws(folder = "data-raw/wahis-raw-outbreak-reports", bucket = "wahis-data")

wahis:::push_aws(folder = "data-raw/wahis-raw-outbreak-pdfs", bucket = "wahis-data")
