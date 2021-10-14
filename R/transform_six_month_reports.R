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

# For summing counts 
sum_na <- function(vec) ifelse(all(is.na(vec)), NA_integer_, sum(as.numeric(vec), na.rm = TRUE))


#' Convert a list of scraped six month reports to a list of table
#' @param six_month_reports a list of outbreak reports produced by [ingest_report]
#' @import dplyr tidyr purrr stringr
#' @importFrom janitor clean_names
#' @importFrom textclean replace_non_ascii
#' @importFrom countrycode countrycode
#' @importFrom assertthat assert_that
#' @export
transform_six_month_reports <- function(six_month_reports) {
    
    message("Transforming six month reports")
    
    # Preprocessing ---------------------------------------------------
    # remove errored reports
    
    six_month_reports2 <- discard(six_month_reports, function(x){
        !is.null(x$ingest_status) && str_detect(x$ingest_status, "ingestion error") |
            !is.null(x$message) && str_detect(x$message, "Endpoint request timed out") 
    })
    if(!length(six_month_reports2)) return(NULL)
    
    # For url lookup
    # https://wahis.oie.int/#/report-smr/view?reportId=20038&period=SEM01&areaId=2&isAquatic=false    
    
    # Extracting data from lists ---------------------------------------------------
    transformed_reports <- imap(six_month_reports2, function(x, y){
        
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
                             rename_with(~str_replace(., "isWild", "isWild0"), suppressWarnings(one_of("isWild"))) 
                         
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
                             ) %>% 
                             distinct()
                         
                         # control measures separately - disease and report specific
                         cm <- dat %>%
                             mutate(specieName0 = if_else(is.na(specieName), groupName, specieName)) %>% 
                             select(
                                 country,
                                 report_id,# report
                                 report_semester, # report semester
                                 report_year, # report year
                                 isAquatic,
                                 areaID,
                                 oieReference,
                                 disease_status,
                                 specieName0,
                                 starts_with("isWild"), 
                                 flex_one_of("diseaseName"), # disease
                                 flex_one_of('cmSummariesList')) %>% 
                             distinct() %>% 
                             rename(isWild1 = isWild) %>% 
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
        
        quant <- map_dfr(out, ~.$quant)
        cm <- map_dfr(out, ~.$control_measures)
        
        return(list("quant" = quant, "control_measures" = cm))
        
    })
    
    transformed_reports <- compact(modify_depth(transformed_reports, 1, compact))
    
    # post-process reports ---------------------------------------
    
    quantitative_reports <- map_dfr(transformed_reports, ~.$quant) 
    
    ## early return if all reports are aquatic
    if(all(quantitative_reports$isAquatic)){
        message("All reports are aquatic. Returning NULL")
        return(NULL)
    }
    
    ## lookup table for country iso3c
    country_iso_lookup <- tibble(country = unique(quantitative_reports$country)) %>%
        mutate(country2 = case_when(
            country == "Central African (Rep.)" ~ "Central African Republic",
            country == "Dominican (Rep.)" ~ "Dominican Republic",
            country == "Serbia and Montenegro" ~ "Serbia",
            TRUE ~ country
        )) %>% 
        mutate(country_iso3c = countrycode::countrycode(sourcevar = country2,origin = "country.name", destination = "iso3c")) %>% 
        select(-country2)
    
    ## general cleaning
    quantitative_reports <- quantitative_reports %>%
        janitor::clean_names() %>%
        mutate(is_wild = coalesce(is_wild0, is_wild)) %>% # when both is_wild0 and is_wild exist, is_wild0 is correct (and is_wild can be wrong)
        mutate(disease_population = ifelse(is_wild, "wild", "domestic")) %>%
        flex_unselect("is_wild0") %>%
        flex_unselect("is_wild") %>% 
        flex_unselect("animal_category") %>% 
        left_join(country_iso_lookup,  by = "country") %>%
        mutate(sub_period_trans = ifelse(is.na(sub_period_trans), report_semester, sub_period_trans)) %>%
        mutate(sub_period_trans = case_when(
            sub_period_trans == "SEM01" ~ "January-June",
            sub_period_trans == "SEM02" ~ "July-September",
            TRUE ~ sub_period_trans)) %>%
        rename(disease = disease_name, disease_status_detail = code,
               taxa = specie_name, cases = quantities_ncase, deaths = quantities_dead,
               serotype = trans_disease_type, adm = area_name, adm_type = template_name,
               period = sub_period_trans, killed_and_disposed = killed_and_displosed) %>% 
        mutate_if(is.character, tolower) %>% 
        mutate(country_iso3c = toupper(country_iso3c)) %>% 
        mutate(report_semester = as.integer(str_remove(report_semester, "sem"))) %>%
        filter(!is_aquatic) %>% # multiple report ids per semesters - requires attention
        drop_na(country_iso3c) # applies to guadaloupe, cueta, melilla
    
    ## careful! disease_status_detail results in dupes 
    quantitative_reports <- quantitative_reports %>% 
        select(-disease_status_detail) %>% 
        distinct()
    
    ## now process control measures
    control_measures <- map_dfr(transformed_reports, ~.$control_measures)  

    control_measures <- control_measures %>%
        clean_names() %>%
        mutate(taxa = coalesce(specie_name,             # preference for species name associated with control measures
                               specie_name0)) %>% # when species name is not associated with control measures, use higher level assignment
        mutate(is_wild = coalesce(is_wild,            # preference for wild designation associated with control measures
                                  coalesce(is_wild0, is_wild1) # this is the heuristic applied to quantitative data earlier (with is_wild now named is_wild1)
        )) %>%
        rename(disease = disease_name) %>%
        mutate(disease_population = ifelse(is_wild, "wild", "domestic")) %>%
        select(-specie_name, -specie_name0, -is_wild0, -is_wild1, -is_wild) %>%
        distinct() %>%
        group_by(country, report_id, report_semester, report_year, is_aquatic, area_id, oie_reference,
                 disease_status, disease, disease_population, taxa) %>%
        summarize(control_measures = paste(measure, collapse = "; "),
                  control_measures_vaccines_administered =  str_flatten(vacc_nb)) %>%
        ungroup() %>% 
        mutate_if(is.character, tolower) %>% 
        mutate(report_semester = as.integer(str_remove(report_semester, "sem")))

    ## lookup table for disease names
    ando_disease_lookup <- readxl::read_xlsx(system.file("diseases", "disease_lookup.xlsx", package = "wahis")) %>%
        mutate(disease = textclean::replace_non_ascii(disease)) %>%
        rename(disease_class = class_desc) %>%
        filter(report == "animal") %>%
        select(-report, -no_match_found) %>%
        mutate_at(.vars = c("ando_id", "preferred_label", "disease_class"), ~na_if(., "NA"))
    
    ## clean disease names on quantitative reports and control measures
    dfs_cleaned <- map(list(quantitative_reports, control_measures), function(df){
        
        df %>%
            mutate(disease = tolower(disease)) %>%
            mutate(disease = trimws(disease)) %>%
            mutate(disease = textclean::replace_non_ascii(disease)) %>%
            mutate(disease = str_remove_all(disease, "\\s*\\([^\\)]+\\)")) %>%
            mutate(disease = str_remove(disease, "virus")) %>%
            mutate(disease = trimws(disease)) %>%
            mutate(disease = str_squish(disease)) %>%
            left_join(ando_disease_lookup, by = "disease") %>%
            mutate(disease = coalesce(preferred_label, disease)) %>%
            select(-preferred_label) %>%
            distinct()
    })
    
    quantitative_reports <- dfs_cleaned[[1]]
    control_measures <- dfs_cleaned[[2]]

    # identify disease names that do not match ando ontology
    diseases_unmatched <- quantitative_reports %>%
        filter(is.na(ando_id)) %>%
        distinct(disease) %>%
        mutate(table = "six_month_report")
    
    ## aggregate over cleaned disease names (note dupes)
    # quantitative_reports %>% 
    #     get_dupes(country, report_semester, report_year, disease_status, disease, serotype, disease_population, taxa, period, adm) 
    quantitative_reports <- quantitative_reports %>% 
        group_by(country, country_iso3c, report_id, report_semester, report_year, is_aquatic, taxa, disease_population,
                 area_id, oie_reference, disease_status, disease, serotype, ando_id, disease_class, 
                 period, adm, adm_type) %>% 
        summarize(susceptible = sum_na(susceptible),
                  cases = sum_na(cases),
                  deaths = sum_na(deaths),
                  killed_and_disposed = sum_na(killed_and_disposed),
                  slaughtered = sum_na(slaughtered),
                  vaccinated = sum_na(vaccinated)) %>% 
        ungroup()
    
    
    ## create detailed table that includes counts by month and/or adm (subnational)
    quantitative_reports_detail <- quantitative_reports %>% 
        filter(disease_status == "present")
    
    ## create summary table that gives status and counts by semester
    quantitative_reports_summary <- quantitative_reports %>% 
        mutate(disease_status_rank = recode(disease_status, "present" = 1, "absent" = 2, "unreported" = 3)) %>%
        group_by(country, country_iso3c, report_id, report_semester, report_year, is_aquatic, taxa, disease_population,
                 area_id, oie_reference, disease, serotype, ando_id, disease_class) %>% 
        summarize(disease_status = min(disease_status_rank),
                  susceptible = sum_na(susceptible),
                  cases = sum_na(cases),
                  deaths = sum_na(deaths),
                  killed_and_disposed = sum_na(killed_and_disposed),
                  slaughtered = sum_na(slaughtered),
                  vaccinated = sum_na(vaccinated)) %>% 
        ungroup() %>% 
        mutate(disease_status = recode(disease_status, '1' = "present", '2' =  "absent", '3' = "unreported"))
    
    
    ## for control measures - aggregate over cleaned disease names (note dupes)
    # control_measures %>%
    #     get_dupes(country, report_semester, report_year, disease_status, disease, disease_population, taxa)
    control_measures <- control_measures %>% 
        group_by(country, report_id, report_semester, report_year, is_aquatic, taxa, disease_population,
                 area_id, oie_reference, disease_status, disease, ando_id, disease_class) %>% 
        summarize(control_measures_vaccines_administered = as.integer(sum_na(suppressWarnings(as.integer(control_measures_vaccines_administered)))),
                  control_measures = str_flatten(control_measures, collapse = "; ")
        ) %>%
        ungroup() %>% 
        mutate(control_measures = map_chr(control_measures, ~str_c(sort(.), collapse = "; "))) %>% 
        mutate(control_measures = na_if(control_measures, "NA"))
    
    ## handling "multiple species"
    # lookup taxa from control measures
    mult_species_lookup <- control_measures %>%
        select(c("country", "report_id", "report_semester", "report_year",
                 "taxa",
                 "is_aquatic", "area_id", "oie_reference", "disease_status", "disease",
                 "disease_population", "ando_id", "disease_class")) %>%
        filter(taxa != "multiple species") %>%
        distinct()
     
    # for absent/unreported multiple species, join with mult_species_lookup to get taxa 
    qrs_mult_spec <- quantitative_reports_summary %>%
        filter(taxa == "multiple species" & disease_status != "present") %>%
        left_join(mult_species_lookup,  by = c("country", "report_id", "report_semester", "report_year",
                                               # "taxa",
                                               "is_aquatic", "area_id", "oie_reference", "disease_status", "disease",
                                               "disease_population", "ando_id", "disease_class")) %>%
        mutate(taxa = ifelse(is.na(taxa.y), taxa.x, taxa.y)) %>%
        select(-taxa.x, -taxa.y)

    # replace absent/unreported multiple species with relevant taxa
    quantitative_reports_summary <- quantitative_reports_summary %>%
        filter(!(taxa == "multiple species" & disease_status != "present")) %>%
        bind_rows(qrs_mult_spec) %>%
        distinct() # take distinct because some diseases were listed as both multiple species and a specific taxa, resulting in dupes when the multiple species is matched
    
    quantitative_reports_summary <- left_join(quantitative_reports_summary, 
                                               control_measures, by = c("country", "report_id", "report_semester", "report_year", 
                                                                        "is_aquatic", "area_id", "oie_reference", "disease_status", "disease",
                                                                        "taxa", 
                                                                        "disease_population", "ando_id", "disease_class")) 
    
    dup_check <- quantitative_reports_summary3 %>% get_dupes(c("country", "report_id", "report_semester", "report_year",
                                                  "is_aquatic", "area_id", "oie_reference", "disease_status", "disease",
                                                  "taxa",
                                                  "disease_population", "ando_id", "disease_class"))
    
    # Export -----------------------------------------------
    wahis_joined <- list("six_month_reports_summary" = quantitative_reports_summary,
                         "six_month_reports_detail" = quantitative_reports_detail,
                         "six_month_reports_diseases_unmatched" = diseases_unmatched)
    
    return(wahis_joined)
    
}

