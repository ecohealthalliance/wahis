flex_unselect <- function(df, x){
    suppressWarnings(select(.data = df, -one_of(x)))
}

flex_pull <- function(df, x){
   if(x %in% colnames(df)){
      return(pull(df, x))
   }else{
       return(NULL)
   }
}

flex_bind_cols <- function(df1, df2){
    suppressMessages(bind_cols(df1, df2))
}

flex_unnest <- function(df, x){
    out <- suppressMessages(suppressWarnings(unnest(data = df, cols = one_of(x), names_repair = "universal")))
    if(nrow(out)==0){
        return(df)
    }else{
        return(out)
    }
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
    tic()
    diseases_present <- imap(six_month_reports2[1:200], function(x, y){
        
        print(y)
        
        dat <- x$occCodePresentDiseaseList %>% 
            as_tibble() 
        
        if(nrow(dat) == 0) return()
        
        dat <- dat %>%    
            flex_unnest("reportDiseaseDtoList")
        
        # diseaseDto
        out <- dat %>% 
            mutate(diseaseName = .$diseaseDto$diseaseName) %>% 
            flex_unselect("diseaseDto")
        
        # templatePreviewDto
        out <- out %>% 
            mutate(templatePreviewDto = templatePreviewDto[[1]]) %>% 
            flex_unnest("templatePreviewDto") %>% 
            flex_unselect("templatePreviewDto") %>% 
            flex_bind_cols(flex_pull(., "periodDto")) %>% 
            flex_unselect("periodDto") %>% 
            flex_unnest("diseaseTypeDto") %>% 
            flex_unnest("areaSpeciesDto") %>% 
            flex_bind_cols(flex_pull(., "diseaseTypeCombDto")) %>% 
            flex_unselect("diseaseTypeCombDto")  %>% 
            select(-ends_with("List")) %>% 
            flex_bind_cols(flex_pull(., "qtySummariesDto")) %>% 
            flex_unselect("qtySummariesDto")
        
        # reportOccCmDto
        out <- out %>% 
            flex_bind_cols(flex_pull(., "reportOccCmDto")) %>% 
            flex_unselect("reportOccCmDto") %>% 
            flex_unnest("occSummariesList") %>% 
            flex_unnest("cmSummariesList") %>% 
            flex_bind_cols(flex_pull(., "occurrenceCodeDto")) %>% 
            flex_unselect("occurrenceCodeDto") %>% 
            flex_unnest("speciesCmList") %>% 
            flex_unnest("cmList")
        
        # map_chr(out, class)
        
        return(out)
        
    })
    toc()
    
    #TODO dupe name handling needs to be post processed -- too slow dealt with upfront , should allow universal fix then just select first occurance
    #TODO field selections (commented out)
    #TODO get countryname
    #TODO add: iso3c, ando_id
    #TODO summarize over control measures, separate out detail and summary
    #TODO add absence, unreported
    
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
    
    
    #  })
}

