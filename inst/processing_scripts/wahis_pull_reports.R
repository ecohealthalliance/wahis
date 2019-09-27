library(pbapply) 
devtools::load_all()
filenames <- list.files("data-raw/raw_wahis_reports",
                        pattern = "*.html",
                        full.names = TRUE)
filenames <- filenames[grepl("2012_sem0", filenames)][1:2]

#wahis <- pblapply(filenames, ingest_wahis_report, cl=40)  

wahis <- list()
for(i in seq_along(filenames)){
    out <- ingest_wahis_report(filenames[[i]])
    wahis[[i]] <- out
}

web_page = filenames[[i]]
web_page = "data-raw/raw_wahis_reports/USA_2012_sem0.html"
