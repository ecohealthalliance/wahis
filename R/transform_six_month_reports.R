#' Convert a list of scraped six month reports to a list of table
#' @param six_month_reports a list of outbreak reports produced by [ingest_report]
#' @import dplyr tidyr purrr stringr
#' @importFrom glue glue_collapse
#' @importFrom janitor clean_names
#' @importFrom lubridate dmy myd ymd
#' @importFrom textclean replace_non_ascii
#' @importFrom countrycode countrycode
#' @importFrom assertthat %has_name%
#' @export

transform_outbreak_reports <- function(six_month_reports) {
    
    message("Transforming six month reports")
    
    # Preprocessing ---------------------------------------------------
    
    six_month_reports2 <- discard(six_month_reports, function(x){
        !is.null(x$ingest_status) && str_detect(x$ingest_status, "ingestion error") |
            !is.null(x$message) && str_detect(x$message, "Endpoint request timed out") 
    })
    if(!length(six_month_reports2)) return(NULL)
    
    # annual_reports_animal_diseases
    # annual_reports_animal_diseases_detail
    # annual_reports_animal_hosts
    # annual_reports_animal_hosts_detail
    
    conn <- repeldata::repel_local_conn()
    annual_reports_animal_diseases <- DBI::dbReadTable(conn, "annual_reports_animal_diseases")
    annual_reports_animal_diseases_detail <- DBI::dbReadTable(conn, "annual_reports_animal_diseases_detail")
    
    annual_reports_animal_hosts <- DBI::dbReadTable(conn, "annual_reports_animal_hosts")
    annual_reports_animal_hosts_detail <- DBI::dbReadTable(conn, "annual_reports_animal_hosts_detail")
    
    #TODO clean disease names
    #TODO I think annual_reports_animal_diseases & annual_reports_animal_diseases_detail can be remove
    #TODO annual_reports_animal_hosts and annual_reports_animal_hosts_detail are what we need -> cases etc by disease, taxa
    
    
    # For url lookup ----------------------------------------------------------
    # https://wahis.oie.int/#/report-smr/view?reportId=20038&period=SEM01&areaId=2&isAquatic=false    
    x = six_month_reports2[[1]]
    x$reportId
    initializationDto <- map_dfc(x$initializationDto, as_tibble)
    initializationDto$period
    initializationDto$isAquatic
    initializationDto$areaId
    
    
    
    # disease present ---------------------------------------------------------
    
    
    diseases_present <- x$occCodePresentDiseaseList %>% 
        as_tibble() %>% 
        rename(taxa = groupName) %>% 
        unnest(reportDiseaseDtoList)  
    
    # 1
    e_reportOccCmDto <- diseases_present$reportOccCmDto 
    glimpse(e_reportOccCmDto)
    
    # 1.1 
    e_occSummariesList <- e_reportOccCmDto %>% 
        select(occSummariesList) %>% 
        unnest(occSummariesList)
    # 1.1.1
    e_occurrenceCodeDto <- e_occSummariesList %>% 
        pull(occurrenceCodeDto)
    assert_that(nrow(e_occSummariesList) == nrow(e_occurrenceCodeDto))
    # 1.1
    e_occSummariesList <- e_occSummariesList %>% 
        select(-occurrenceCodeDto) %>% 
        bind_cols(e_occurrenceCodeDto)
    glimpse(e_occSummariesList)
    
    # 1.2
    e_cmSummariesList <- e_reportOccCmDto %>% 
        select(cmSummariesList) %>% 
        unnest(cmSummariesList)
    # 1.2.1
    e_speciesCmList <- e_cmSummariesList %>% 
        pull(speciesCmList) %>% 
        map_dfr(bind_rows) %>% 
        unnest(cmList)
}

