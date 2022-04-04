devtools::load_all()
library(scrapetools)
library(tictoc)

# # get 6 month reports list -----------------------------------------------
report_list <- scrape_six_month_report_list()

reports_to_get <- report_list %>%
    select(report_id) %>%
    mutate(url = paste0("https://wahis.oie.int/smr/pi/report/", report_id, "?format=preview")) %>% 
    slice(sample(nrow(.), 500, replace = F))

# example report view - Afghanistan 
# https://wahis.oie.int/#/report-smr/view?reportId=20038&period=SEM01&areaId=2&isAquatic=false  # formatted
# https://wahis.oie.int/smr/pi/report/20038?format=preview # api

# Pulling reports ----------------------------
message("Pulling ", nrow(reports_to_get), " reports")

tic()
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
toc()

# Save ingested files   ------------------------------------------------------
# dir_create(here::here("data-processed"))
# readr::write_rds(report_resps, here::here("data-processed", "report_resps_six_month.rds"), compress = "xz", compression = 9L)
# report_resps <- read_rds(here::here("data-processed", "report_resps_six_month.rds"))
report_resps <- reduce(report_resps, c)

# Transform files   ------------------------------------------------------
# tables
tic()
six_month_report_tables <- split(report_resps, (1:length(report_resps)-1) %/% 1000) %>% # batching by 1000s (probably only necessary for initial run)
    map(., transform_six_month_reports)
six_month_report_tables <- reduce(six_month_report_tables, c)

readr::write_rds(six_month_report_tables, here::here("data-processed", "transformed_six_month.rds"), compress = "xz", compression = 9L)
toc()

six_month_report_tables <- readr::read_rds(here::here("data-processed", "transformed_six_month.rds"))

quantitative_reports_summary <- reduce(six_month_report_tables[names(six_month_report_tables)== "quantitative_reports_summary" ],
                                       bind_rows)

readr::write_rds(quantitative_reports_summary, here::here("data-processed", "transformed_six_month_quantitative_reports_summary.rds"), compress = "xz", compression = 9L)
