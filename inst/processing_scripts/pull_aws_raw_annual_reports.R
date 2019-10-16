#!/usr/bin/env Rscript

# Script to sync data files
# See https://ecohealthalliance.github.io/eha-ma-handbook/11-cloud-computing-services.html
# For credentials setup

wahis:::pull_aws(bucket = "wahis-data", object = "wahis-raw-annual-reports.tar.xz", dir = ".")
