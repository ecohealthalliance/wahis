devtools::load_all()
library(scrapetools)

# get 6 month reports list -----------------------------------------------
report_list <- scrape_six_month_report_list() 

reports_to_get <- report_list %>% 
    select(report_id) %>% 
    mutate(url = paste0("https://wahis.oie.int/smr/pi/report/", report_id, "?format=preview"))

# Pulling reports ----------------------------
message("Pulling ", nrow(reports_to_get), " reports")

report_resps <- split(reports_to_get, (1:nrow(reports_to_get)-1) %/% 100) %>% # batching by 100s
    map(function(reports_to_get_split){
        map_curl(
            urls = reports_to_get_split$url,
            .f = function(x) wahis::safe_ingest(x),
            .host_con = 8L,
            .delay = 0.5,
            .handle_opts = list(low_speed_limit = 100, low_speed_time = 300), # bytes/sec
            .retry = 2,
            .handle_headers = list(`Accept-Language` = "en")
        )
    })

# Save ingested files   ------------------------------------------------------
# dir_create(here::here("data-processed"))
readr::write_rds(report_resps, here::here("data-processed", "report_resps_six_month.rds"), compress = "xz", compression = 9L)
# report_resps <- read_rds(here::here("data-processed", "report_resps_six_month.rds"))
# report_resps <- reduce(report_resps, c)

