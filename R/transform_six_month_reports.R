replace_x <- function(x, replacement = NA_character_) {
    if (length(x) == 0 || length(x[[1]]) == 0) {
        replacement
    } else {
        x
    }
}

one_of_sw <- function(x){
    suppressWarnings(one_of(x))
}


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

transform_six_month_reports <- function(six_month_reports) {
    
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
    
    #TODO I think annual_reports_animal_diseases & annual_reports_animal_diseases_detail can be removed
    #TODO annual_reports_animal_hosts and annual_reports_animal_hosts_detail are what we need -> cases etc by disease, taxa
    
    
    # For url lookup ----------------------------------------------------------
    # https://wahis.oie.int/#/report-smr/view?reportId=20038&period=SEM01&areaId=2&isAquatic=false    
    # x = six_month_reports2[[1]]
    # x$reportId
    # initializationDto <- map_dfc(x$initializationDto, as_tibble)
    # initializationDto$period
    # initializationDto$isAquatic
    # initializationDto$areaId
    
    # disease present ---------------------------------------------------------
    
    diseases_present <- imap(six_month_reports2[1:20], function(x, i){
        
        print(i)
        dat <- x$occCodePresentDiseaseList %>% 
            as_tibble() 
        
        if(nrow(dat) == 0) return()
        
        dat <- dat %>%    
            unnest(reportDiseaseDtoList) 
        
        out <- dat
        i <- 1
        while(i <= ncol(out)){
            
            col_c <- class(pull(out, i))
            
            if(!col_c  %in% c("list", "data.frame")){
                i <- i + 1
                next()
            }
            
            col_n <- names(out[,i])
            
            if(col_c  == "data.frame"){
                out <- out %>% 
                    bind_cols(pull(., col_n)) %>% 
                    select(-!!col_n)
                i <- i # do not iterate
            }
            
            if(col_c  == "list"){
                if(all(map_lgl(pull(out, col_n), is.null))|length(compact(pull(out, col_n))) == 0) {
                    out <- out %>% 
                        select(-!!col_n)
                    i <- i # do not iterate
                    next()
                }
                
                out <- out %>%
                    purrr::modify_depth(2, replace_x) %>% 
                    unnest(col_n, names_repair = "universal") 
                
                i <- i # do not iterate
            }
            
            if(nrow(out)==0) stop()
        }
        
        out <- out %>% 
            mutate(report_id = x$reportId,
                   period = x$initializationDto$period$period,
                   year = x$initializationDto$period$year,
                   isAquatic = x$initializationDto$animalTypeDto$isAquatic,
                   areaID = x$initializationDto$areaDto$areaId,
                   oieReference = x$oieReference) %>% 
            select(
                # country
                report_id,# report
                period, # report semester
                year, # report year
                isAquatic,
                areaID,
                oieReference,
                one_of_sw("diseaseName"), # disease
                starts_with("isWild"),# disease population
                one_of_sw("isAbsenceOfOutbreak"), # disease_status
                one_of_sw("animalCategoryDomestic"),
                one_of_sw("animalCategoryWild"),
                one_of_sw("code"),
                starts_with("specieName"), # taxa
                one_of_sw("subPeriodTrans"), # period
                # temporal_scale
                starts_with("areaName"),  # adm
                one_of_sw("templateName"), # adm type
                one_of_sw("susceptible"), # susceptible_detail
                one_of_sw("quantitiesNcase"), # cases_detail
                one_of_sw("quantitiesDead"), # deaths_detail
                one_of_sw("killedAndDisplosed"), # killed_and_disposed_of_detail
                one_of_sw("slaughtered"), # slaughtered_detail
                one_of_sw("vaccNb"), # vaccination_in_response_to_the_outbreak_detail
                one_of_sw("vaccinated"),
                # serotype
                # measuring_units
                one_of_sw("measure") # control measures
            )
        
        out$specieName...12 == out$specieName...58
        
        
        return(out)
        
    })
    
    #TODO profiling speed up
    #TODO pull countryname
    #TODO add: iso3c, ando_id
    #TODO summarize over control measures, separate out detail and summary
    
    
}

