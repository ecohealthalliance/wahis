devtools::load_all()

# get outbreak reports list -----------------------------------------------

report_list <- scrape_outbreak_report_list() 

reports_to_get <- report_list %>%
    select(report_info_id, # url
           outbreak_thread_id = event_id_oie_reference) # thread number

lookup_outbreak_thread_url <-  report_list %>% 
    filter(report_type == "IN") %>% 
    select(outbreak_thread_id = event_id_oie_reference, url_outbreak_thread_id = report_info_id) 

reports_to_get <- left_join(reports_to_get, lookup_outbreak_thread_url, by = "outbreak_thread_id") %>% 
    mutate(url =  paste0("https://wahis.oie.int/pi/getReport/", report_info_id))

# Pulling reports ----------------------------
message("Pulling ", nrow(reports_to_get), " reports")

report_resps <- split(reports_to_get, (1:nrow(reports_to_get)-1) %/% 100) %>% # batching by 100s
    map(function(reports_to_get_split){
        map_curl(
            urls = reports_to_get_split$url,
            .f = function(x) wahis::safe_ingest_outbreak(x),
            .host_con = 8L, # can turn up
            .delay = 0.5,
            #.timeout = nrow(reports_to_get)*120L,
            .handle_opts = list(low_speed_limit = 100, low_speed_time = 300), # bytes/sec
            .retry = 2
            # need to add language here?
        )
    })

# Save ingested files   ------------------------------------------------------
# dir_create(here::here("data-processed"))
# readr::write_rds(report_resps, here::here("data-processed", "report_resps_outbreak.rds.rds"), compress = "xz", compression = 9L)
# report_resps <- read_rds(here::here("data-processed", "report_resps_outbreak.rds"))
report_resps <- reduce(report_resps, c)

# Transform files   ------------------------------------------------------
# tables
outbreak_report_tables <- split(report_resps, (1:length(report_resps)-1) %/% 1000) %>% # batching by 1000s (probably only necessary for initial run)
    map(., transform_outbreak_reports, report_list)

outbreak_report_tables <- transpose(outbreak_report_tables) %>%
    map(function(x) reduce(x, bind_rows))
