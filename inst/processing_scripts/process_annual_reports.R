library(pbapply) 
library(wahis)
#devtools::load_all()

# Pull files --------------------------------------------------------------

# List all file names
filenames <- list.files(here::here("data-raw/raw_wahis_annual_reports"),
                        pattern = "*.html",
                        full.names = TRUE)

# Run scraper (~1 hr)
wahis <- pblapply(filenames, safe_ingest, cl = parallel::detectCores())  

# Save
readr::write_rds(wahis, here::here("data", "wahis.rds"))
