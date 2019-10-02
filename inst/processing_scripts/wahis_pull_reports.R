library(pbapply) 
devtools::load_all()
filenames <- list.files("data-raw/raw_wahis_reports",
                        pattern = "*.html",
                        full.names = TRUE)
filenames <- filenames[grepl("2014_sem0", filenames)]
filenames <- filenames[sample(length(filenames), 100)]

#wahis <- pblapply(filenames, ingest_wahis_report, cl=40)  

wahis <- list()
for(i in seq_along(filenames)){
    out <- ingest_wahis_report(filenames[[i]])
    wahis[[i]] <- out
}

web_page = filenames[[i]]
web_page = "data-raw/raw_wahis_reports/USA_2012_sem0.html"

diseases <- map_df(wahis, function(x){
    if(x=="report error"){return()}
    x$diseases_present %>%
        filter(!is.na(serotype_s), !serotype_s %in% c("...", "No")) %>%
        mutate(country = x$country) %>%
        select(country, everything()) %>%
        arrange(disease)
})
nrow(diseases)

diseases <- diseases %>% arrange(disease)
