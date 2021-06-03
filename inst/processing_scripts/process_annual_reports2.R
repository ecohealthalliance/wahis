devtools::load_all()
library(scrapetools)

# get 6 month reports list -----------------------------------------------
report_list <- scrape_six_month_report_list() 

reports_to_get <- report_list %>% 
    select(report_id) %>% 
    mutate(url = paste0("https://wahis.oie.int/smr/pi/report/", report_id, "?format=preview"))

# Pulling reports ----------------------------
message("Pulling ", nrow(reports_to_get), " reports")

# this works
req <- httr::GET(url = reports_to_get$url[[1]],
                 httr::add_headers(`Accept-Language` = "en"))
content(req, as="parsed")

#TODO translate into using map_curl
