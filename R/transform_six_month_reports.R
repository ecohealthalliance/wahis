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
    
    # library(profvis)
    # profvis({
    diseases_present <- map(six_month_reports2[1:20], function(x){
        
        dat <- x$occCodePresentDiseaseList %>% 
            as_tibble() 
        
        if(nrow(dat) == 0) return()
        
        dat <- dat %>%    
            unnest(reportDiseaseDtoList)
        
        tic()
        # diseaseDto
        out <- dat %>% 
            mutate(diseaseName = .$diseaseDto$diseaseName) %>% 
            select(-diseaseDto)
        
        # templatePreviewDto
        out <- out %>% 
            mutate(templatePreviewDto = templatePreviewDto[[1]]) %>% 
            unnest(templatePreviewDto) %>% 
            select(-templatePreviewDto) %>% 
            bind_cols(pull(., periodDto)) %>% 
            select(-periodDto) %>% 
            unnest(diseaseTypeDto) %>% 
            unnest(areaSpeciesDto) %>% 
            bind_cols(pull(., diseaseTypeCombDto)) %>% 
            select(-diseaseTypeCombDto)  %>% 
            select(-ends_with("List"))
        
        out$qtySummariesDto <- out$qtySummariesDto %>% 
            select(-intersect(names(out), names(.)))
        
        out <- out %>% 
            bind_cols(pull(., qtySummariesDto)) %>% 
            select(-qtySummariesDto) 
        
        # reportOccCmDto
        out <- out %>% 
            bind_cols(pull(., reportOccCmDto)) %>% 
            select(-reportOccCmDto)
        
        out$occSummariesList <- out$occSummariesList %>% 
            map(., ~select(., -isWild))
        
        out$cmSummariesList <- out$cmSummariesList %>% 
            map(., ~select(., -isWild))
        

        out <- out %>% 
            unnest(occSummariesList, names_repair = "universal") %>% 
            unnest(cmSummariesList, names_repair = "universal")
        
        out$occurrenceCodeDto <-  out$occurrenceCodeDto %>% 
            select(-intersect(names(out), names(.)))
        
        out$speciesCmList <- out$speciesCmList %>% 
            map(., ~select(., -specieId, -specieName))

        
        out <- out %>% 
            bind_cols(pull(., occurrenceCodeDto)) %>% 
            select(-occurrenceCodeDto) %>% 
            unnest(speciesCmList, names_repair = "universal") 
        
        out$cmList <- out$cmList %>% 
            map(., ~select(., -nature, -status))
        
        out <- out %>% 
            unnest(cmList, names_repair = "universal")
        
            toc()
            
       # map_chr(out, class)
        

        # out <- out %>% 
        #     mutate(report_id = x$reportId,
        #            period = x$initializationDto$period$period,
        #            year = x$initializationDto$period$year,
        #            isAquatic = x$initializationDto$animalTypeDto$isAquatic,
        #            areaID = x$initializationDto$areaDto$areaId,
        #            oieReference = x$oieReference) %>% 
        #     select(
        #         # country
        #         report_id,# report
        #         period, # report semester
        #         year, # report year
        #         isAquatic,
        #         areaID,
        #         oieReference,
        #         one_of_sw("diseaseName"), # disease
        #         starts_with("isWild"),# disease population
        #         one_of_sw("isAbsenceOfOutbreak"), # disease_status
        #         one_of_sw("animalCategoryDomestic"),
        #         one_of_sw("animalCategoryWild"),
        #         one_of_sw("code"),
        #         starts_with("specieName"), # taxa
        #         one_of_sw("subPeriodTrans"), # period
        #         # temporal_scale
        #         starts_with("areaName"),  # adm
        #         one_of_sw("templateName"), # adm type
        #         one_of_sw("susceptible"), # susceptible_detail
        #         one_of_sw("quantitiesNcase"), # cases_detail
        #         one_of_sw("quantitiesDead"), # deaths_detail
        #         one_of_sw("killedAndDisplosed"), # killed_and_disposed_of_detail
        #         one_of_sw("slaughtered"), # slaughtered_detail
        #         one_of_sw("vaccNb"), # vaccination_in_response_to_the_outbreak_detail
        #         one_of_sw("vaccinated"),
        #         # serotype
        #         # measuring_units
        #         one_of_sw("measure") # control measures
        #     )
        
        return(out)
        
    })
    
    #TODO profiling speed up - pull things out directly - in progress - need to make all selections robust and handle dup names
    #TODO dupe name handling needs to be post processed -- too slow dealt with upfront , should allow universal fix then just select first occurance
    #TODO field selections (commented out)
    #TODO get countryname
    #TODO add: iso3c, ando_id
    #TODO summarize over control measures, separate out detail and summary
    #TODO add absence, unreported
    
    #  })
}

