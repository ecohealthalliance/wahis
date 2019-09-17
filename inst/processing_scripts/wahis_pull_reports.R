library(pbapply)
devtools::load_all()
filenames <- list.files("data-raw/raw_wahis_reports",
                        pattern = "*.html",
                        full.names = TRUE)[11:12]
wahis <- pblapply(filenames, ingest_wahis_report, cl=40)  
