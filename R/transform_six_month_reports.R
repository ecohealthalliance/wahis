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
    out <- suppressMessages(suppressWarnings(unnest(data = df, cols = one_of(x), names_repair = "universal", keep_empty = TRUE)))
    if(nrow(out)==0){
        return(df)
    }else{
        return(out)
    }
}

flex_one_of <- function(x){
    suppressWarnings(one_of(x))
}

assert_distinct <- function(df, x){
    
    out <- df %>% 
        select(starts_with(x)) 
    
    if(ncol(out)==0) return(TRUE)
    
    out <- out %>% 
        distinct() %>% 
        t() %>% 
        as_tibble() %>% 
        distinct() 
    
    assert_that(nrow(out) ==1)
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
    
    # conn <- repeldata::repel_local_conn()
    # annual_reports_animal_diseases <- DBI::dbReadTable(conn, "annual_reports_animal_diseases")
    # annual_reports_animal_diseases_detail <- DBI::dbReadTable(conn, "annual_reports_animal_diseases_detail")
    # 
    # annual_reports_animal_hosts <- DBI::dbReadTable(conn, "annual_reports_animal_hosts")
    # annual_reports_animal_hosts_detail <- DBI::dbReadTable(conn, "annual_reports_animal_hosts_detail")
    
    
    # For url lookup ----------------------------------------------------------
    # https://wahis.oie.int/#/report-smr/view?reportId=20038&period=SEM01&areaId=2&isAquatic=false    
    # x = six_month_reports2[[1]]
    # x$reportId
    # initializationDto <- map_dfc(x$initializationDto, as_tibble)
    # initializationDto$period
    # initializationDto$isAquatic
    # initializationDto$areaId
    
    transformed_reports <- imap(six_month_reports2[1:100], function(x, y){
        
        print(y)
        
        out <- map(c("occCodePresentDiseaseList", 
                     "occCodeAbsentDiseaseList",
                     "occCodeNoInfoDiseaseList"), function(xx){
                         
                         disease_status <- switch(xx,
                                                  "occCodePresentDiseaseList" = "present",
                                                  "occCodeAbsentDiseaseList" = "absent",
                                                  "occCodeNoInfoDiseaseList" = "unreported")
                         
                         dat <- x[[xx]] %>% 
                             as_tibble() 
                         
                         if(nrow(dat) == 0) return()
                         
                         # top level info
                         dat <-  dat %>% 
                             mutate(report_id = x$reportId,
                                    country =  na.omit(x$reportInfoDto$reportUserInfoDtoList$countryOrTerritory),
                                    report_semester = x$initializationDto$period$period,
                                    report_year = x$initializationDto$period$year,
                                    isAquatic = x$initializationDto$animalTypeDto$isAquatic,
                                    areaID = x$initializationDto$areaDto$areaId,
                                    oieReference = x$oieReference,
                                    disease_status = disease_status) %>% 
                             relocate(suppressWarnings(one_of("reportDiseaseDtoList")), .after = everything())
                         
                         dat <- dat %>%    
                             flex_unnest("reportDiseaseDtoList")
                         
                         # diseaseDto
                         dat <- dat %>% 
                             mutate(diseaseName = .$diseaseDto$diseaseName) %>% 
                             flex_unselect("diseaseDto")
                         
                         # templatePreviewDto
                         dat <- dat %>% 
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
                         dat <- dat %>% 
                             flex_bind_cols(flex_pull(., "reportOccCmDto")) %>% 
                             flex_unselect("reportOccCmDto") %>% 
                             flex_unnest("occSummariesList") %>% 
                             flex_bind_cols(flex_pull(., "occurrenceCodeDto")) %>% 
                             flex_unselect("occurrenceCodeDto")
                         
                         # check mismatches (dont worry about nature, status, newOutbreak, totalOutbreak because they are being excluded)
                         assert_distinct(dat, "areaName")
                         areaname <- colnames(dat)[str_detect(colnames(dat), "areaName")]
                         if(length(areaname) > 1) { 
                             areaname_remove <- areaname[-1]
                             dat <- dat %>% 
                                 select(-all_of(areaname_remove) )
                             
                             if(length(areaname) > 0){
                                 dat <- dat %>% 
                                     rename(areaName = areaname[[1]]) 
                             }
                         }
                         
                         # quant data select
                         quant <- dat %>% 
                             mutate_at(vars(flex_one_of("specieName")), ~if_else(is.na(.), groupName, .)) %>% 
                             select(
                                 country, # country
                                 report_id,# report
                                 report_semester, # report semester
                                 report_year, # report year
                                 isAquatic,
                                 areaID,
                                 oieReference,
                                 disease_status,
                                 # starts_with("isWild"), # lots of inconsistencies, eg "https://wahis.oie.int/smr/pi/report/24848?format=preview"
                                 flex_one_of("animalCategory"),
                                 flex_one_of("diseaseName"), # disease
                                 flex_one_of("code"),
                                 flex_one_of("specieName"), # taxa
                                 flex_one_of("subPeriodTrans"), # period
                                 flex_one_of("areaName"),  # adm
                                 flex_one_of("templateName"), # adm type
                                 flex_one_of("susceptible"), # susceptible_detail
                                 flex_one_of("quantitiesNcase"), # cases_detail
                                 flex_one_of("quantitiesDead"), # deaths_detail
                                 flex_one_of("killedAndDisplosed"), # killed_and_disposed_of_detail
                                 flex_one_of("slaughtered"), # slaughtered_detail
                                 flex_one_of("vaccinated"),
                                 flex_one_of("transDiseaseType")  # serotype
                                 #  flex_one_of("quantatiesUnit") # measuring_units
                                 # flex_one_of("measure") # control measures
                                 #flex_one_of("vaccNb") # vaccination_in_response_to_the_outbreak_detail
                             ) %>% 
                             distinct()
                         
                         # control measures separately - disease and report specific
                         cm <- dat %>%
                             select(
                                 country,
                                 report_id,# report
                                 report_semester, # report semester
                                 report_year, # report year
                                 isAquatic,
                                 areaID,
                                 oieReference,
                                 disease_status,
                                 flex_one_of("diseaseName"), # disease
                                 flex_one_of('cmSummariesList')) %>% 
                             distinct() %>% 
                             flex_unnest('cmSummariesList') %>% 
                             flex_unnest("speciesCmList") %>% 
                             flex_unnest("cmList") %>% 
                             select(
                                 -flex_one_of("cmId"),
                                 -flex_one_of("nature"),
                                 -flex_one_of("status"),
                                 -flex_one_of("smrCmdId"),
                                 -flex_one_of("cmKey"),
                                 -flex_one_of("specieId")
                             ) %>% 
                             distinct()
                         
                         
                         return(list("quant" = quant, "control_measures" = cm))
                         
                     }) %>% 
            set_names(c("present", "absent", "unreported"))
        
        ###TODO join quant and cm separately, over pres/abs/unr
        quant <- map_dfr(out, ~.$quant)
        cm <- map_dfr(out, ~.$control_measures)
        
        return(list("quant" = quant, "control_measures" = cm))
        
    })
    
    transformed_reports <- compact(modify_depth(transformed_reports, 1, compact))
    quantitative_reports <- map_dfr(transformed_reports, ~.$quant)
    control_measures <- map_dfr(transformed_reports, ~.$control_measures)
    
    disease_name_lookup <- tibble(country = unique(quantitative_reports$country)) %>% 
        mutate(country_iso3c = countrycode::countrycode(sourcevar = country,origin = "country.name", destination = "iso3c"))
    
    quantitative_reports2 <- quantitative_reports %>% 
        janitor::clean_names() %>% 
        left_join(disease_name_lookup,  by = "country") 
    
    #TODO post process: 
    # wild  - still working on this - need to not lose info from isWild cols
    # ando_id
    # renaming vars (compare to existing)
    
}

