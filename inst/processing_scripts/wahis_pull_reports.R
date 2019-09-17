library(pbapply)
devtools::load_all()
filenames <- list.files("data-raw/raw_wahis_reports",
                        pattern = "*.html",
                        full.names = TRUE)[11:12]
filenames <- "data-raw/raw_wahis_reports/USA_2017_sem0.html"
wahis <- pblapply(filenames, ingest_wahis_report, cl=40)  
