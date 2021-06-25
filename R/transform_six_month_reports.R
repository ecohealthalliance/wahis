#' Support function for flexible unselecting (does not require field to exist)
#' @param df A data frame, data frame extension (e.g. a tibble), or a lazy data frame (e.g. from dbplyr or dtplyr). 
#' @param x One or more unquoted expressions separated by commas. Variable names can be used as if they were positions in the data frame, so expressions like x:y can be used to select a range of variables.
#' @import dplyr
#' @noRd
flex_unselect <- function(df, x){
    suppressWarnings(select(.data = df, -one_of(x)))
}

#' Support function for flexible pulling (does not require field to exist)
#' @param df A data frame, data frame extension (e.g. a tibble), or a lazy data frame (e.g. from dbplyr or dtplyr). 
#' @param x One or more unquoted expressions separated by commas. Variable names can be used as if they were positions in the data frame, so expressions like x:y can be used to select a range of variables.
#' @import dplyr
#' @noRd
flex_pull <- function(df, x){
    if(x %in% colnames(df)){
        return(pull(df, x))
    }else{
        return(NULL)
    }
}

#' Support function for flexible binding (allows duplicate columns to exist)
#' @param df1 A data frame, data frame extension (e.g. a tibble), or a lazy data frame (e.g. from dbplyr or dtplyr). 
#' @param df2 A data frame, data frame extension (e.g. a tibble), or a lazy data frame (e.g. from dbplyr or dtplyr). 
#' @import dplyr
#' @noRd
flex_bind_cols <- function(df1, df2){
    suppressMessages(bind_cols(df1, df2))
}

#' Support function for flexible unnesting (does not require field to exist)
#' @param df A data frame, data frame extension (e.g. a tibble), or a lazy data frame (e.g. from dbplyr or dtplyr). 
#' @param x One or more unquoted expressions separated by commas. Variable names can be used as if they were positions in the data frame, so expressions like x:y can be used to select a range of variables.
#' @import tidyr
#' @noRd
flex_unnest <- function(df, x){
    out <- suppressMessages(suppressWarnings(unnest(data = df, cols = one_of(x), names_repair = "universal", keep_empty = TRUE)))
    if(nrow(out)==0){
        return(df)
    }else{
        return(out)
    }
}

#' Support function for flexible one_of selection (does not throw warning when field does not exist)
#' @param df A data frame, data frame extension (e.g. a tibble), or a lazy data frame (e.g. from dbplyr or dtplyr). 
#' @param x One or more unquoted expressions separated by commas. Variable names can be used as if they were positions in the data frame, so expressions like x:y can be used to select a range of variables.
#' @import dplyr
#' @noRd
flex_one_of <- function(x){
    suppressWarnings(one_of(x))
}

#' Support function to check that columns with identical names have identical info
#' @param df A data frame, data frame extension (e.g. a tibble), or a lazy data frame (e.g. from dbplyr or dtplyr). 
#' @param x One or more unquoted expressions separated by commas. Variable names can be used as if they were positions in the data frame, so expressions like x:y can be used to select a range of variables.
#' @import dplyr
#' @noRd
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
                             flex_unselect("qtySummariesDto") %>% 
                             rename_with( ~str_replace(., "isWild", "isWild0"), suppressWarnings(one_of("isWild"))) 

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
                        if(!"specieName" %in% colnames(dat)) dat$specieName <- dat$groupName
                         quant <- dat %>% 
                             mutate(specieName = if_else(is.na(specieName), groupName, specieName)) %>% 
                             select(
                                 country, # country
                                 report_id,# report
                                 report_semester, # report semester
                                 report_year, # report year
                                 isAquatic,
                                 areaID,
                                 oieReference,
                                 disease_status,
                                 starts_with("isWild"), 
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
    
    quantitative_reports <- quantitative_reports %>% 
        janitor::clean_names() %>% 
        mutate(is_wild = coalesce(is_wild0, is_wild)) %>% # when both is_wild0 and is_wild exist, is_wild0 is correct (and is_wild can be wrong)
        mutate(disease_population = ifelse(is_wild, "wild", "domestic")) %>% 
        select(-is_wild0, -is_wild, -animal_category) %>% 
        left_join(disease_name_lookup,  by = "country") %>% 
        mutate(sub_period_trans = ifelse(is.na(sub_period_trans), report_semester, sub_period_trans)) %>% 
        mutate(sub_period_trans = case_when(
            sub_period_trans == "SEM01" ~ "January-June",
            sub_period_trans == "SEM02" ~ "July-September",
            TRUE ~ sub_period_trans)) %>% 
        rename(disease = disease_name, disease_status_detail = code, 
               taxa = specie_name, cases = quantities_ncase, deaths = quantities_dead,
               serotype = trans_disease_type, adm = area_name, adm_type = template_name,
               period = sub_period_trans)
    
    ando_disease_lookup <- readxl::read_xlsx(system.file("diseases", "disease_lookup.xlsx", package = "wahis")) %>%
        mutate(disease = textclean::replace_non_ascii(disease)) %>%
        rename(disease_class = class_desc) %>%
        filter(report == "animal") %>%
        select(-report, -no_match_found) %>%
        mutate_at(.vars = c("ando_id", "preferred_label", "disease_class"), ~na_if(., "NA"))
    
    quantitative_reports <- quantitative_reports %>%
        mutate(disease = tolower(disease)) %>% 
        mutate(disease = trimws(disease)) %>%
        mutate(disease = textclean::replace_non_ascii(disease)) %>%
        mutate(disease = ifelse(disease == "", causal_agent, disease)) %>%
        mutate(disease = str_remove_all(disease, "\\s*\\([^\\)]+\\)")) %>% 
        mutate(disease = str_remove(disease, "virus")) %>% 
        mutate(disease = trimws(disease)) %>%
        mutate(disease = str_squish(disease)) %>% 
        left_join(ando_disease_lookup, by = "disease") %>%
        mutate(disease = coalesce(preferred_label, disease)) %>%
        select(-preferred_label) %>%
        distinct()
    
    diseases_unmatched <- quantitative_reports %>%
        filter(is.na(ando_id)) %>%
        distinct(disease) %>%
        mutate(table = "six_month_report")
    
    #TODO post process: 
    # general present vs detail
    # control measures cleaning
    
}

