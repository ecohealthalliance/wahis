library(pbapply) 
devtools::load_all()
filenames <- list.files("data-raw/raw_wahis_reports",
                        pattern = "*.html",
                        full.names = TRUE)
filenames <- filenames[grepl("2015_sem0", filenames)]

#wahis <- pblapply(filenames, ingest_wahis_report, cl=40)  

wahis <- list()
for(i in seq_along(filenames)){
    out <- ingest_wahis_report(filenames[[i]])
    wahis[[i]] <- out
}

web_page = filenames[[i]]
