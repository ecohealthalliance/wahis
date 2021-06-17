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
    
    # disease present ---------------------------------------------------------
    tic()
    diseases_present <- imap(six_month_reports2[1:200], function(x, y){
        
        print(y)
        
       
        
        dat <- x$occCodePresentDiseaseList %>% 
            as_tibble() 
        
        if(nrow(dat) == 0) return()
        
        # top level info
        dat <-  dat %>% 
            mutate(report_id = x$reportId,
                   report_country =  na.omit(x$reportInfoDto$reportUserInfoDtoList$countryOrTerritory),
                   report_semester = x$initializationDto$period$period,
                   report_year = x$initializationDto$period$year,
                   isAquatic = x$initializationDto$animalTypeDto$isAquatic,
                   areaID = x$initializationDto$areaDto$areaId,
                   oieReference = x$oieReference) %>% 
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
            flex_unnest("cmSummariesList") %>% 
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
            select(
                report_country, # country
                report_id,# report
                report_semester, # report semester
                report_year, # report year
                isAquatic,
                areaID,
                oieReference,
                # starts_with("isWild"), # lots of inconsistencies, eg "https://wahis.oie.int/smr/pi/report/24848?format=preview"
                flex_one_of("animalCategory"),
                flex_one_of("diseaseName"), # disease
                flex_one_of("isAbsenceOfOutbreak"), # disease_status
                flex_one_of("animalCategoryDomestic"),
                flex_one_of("animalCategoryWild"),
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
                flex_one_of("transDiseaseType"),  # serotype
                    flex_one_of("speciesCmList")
                #  flex_one_of("quantatiesUnit") # measuring_units
                # flex_one_of("measure") # control measures
                #flex_one_of("vaccNb") # vaccination_in_response_to_the_outbreak_detail
            )
        
        # control measures separately - disease and report specific
        cm <- dat %>%
            select(
                report_country,
                report_id,# report
                report_semester, # report semester
                report_year, # report year
                isAquatic,
                areaID,
                oieReference,
                diseaseName,
                flex_one_of('speciesCmList')) %>% 
            distinct() %>% 
            flex_unnest("speciesCmList") %>% 
            flex_unnest("cmList") %>% 
            select(
               -flex_one_of("cmId"),
               -flex_one_of("nature"),
               -flex_one_of("status"),
               -flex_one_of("smrCmdId"),
               -flex_one_of("cmKey"),
               -flex_one_of("specieId")
            )
        
        # map_chr(dat, class)
        
        return(list(quant, cm))
        
    })
    toc()
    
    diseases_present <- compact(diseases_present)
    
    #TODO post process, wild or not
    #TODO add: iso3c, ando_id
    #TODO add absence, unreported
    #TODO make yearly summary?
    
}

