#!/usr/bin/env Rscript

# Script to sync data files
# See https://ecohealthalliance.github.io/eha-ma-handbook/11-cloud-computing-services.html
# For credentials setup

wahis:::push_aws(folder = "data-raw", filename = "wahis-raw-annual-reports", bucket = "wahis-data")
