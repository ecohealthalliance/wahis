#' Download fao taxa population data
#' @param directory where taxa population is saved
#' @import here
#' @export
download_taxa_population <- function(directory){
    suppressWarnings(dir.create(here(directory, "fao-taxa-population")))
    download.file("http://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Livestock_E_All_Data_(Normalized).zip",
                  destfile =  here(directory, "fao-taxa-population/fao-taxa-population.zip"))
}


#' Transform fao taxa data 
#' @param directory where taxa population is saved
#' @import dplyr tidyr here
#' @importFrom countrycode countrycode
#' @importFrom janitor clean_names
#' @importFrom vroom vroom
#' @export

transform_taxa_population <- function(directory){
    
    setwd(here(directory, "fao-taxa-population"))
    unzip(here(directory, "fao-taxa-population/fao-taxa-population.zip"))
    setwd(here())
    
    fao <- vroom(here(directory, "fao-taxa-population/Production_Livestock_E_All_Data_(Normalized).csv"), 
                    col_types = cols(
                        `Area Code` = col_skip(),
                        Area = col_character(),
                        `Item Code` = col_skip(),
                        Item = col_character(),
                        `Element Code` = col_skip(),
                        Element = col_skip(),
                        `Year Code` = col_skip(),
                        Year = col_double(),
                        Unit = col_character(),
                        Value = col_double(),
                        Flag = col_skip()
                    ), 
                 locale = locale(encoding = "Latin1")) %>%
        janitor::clean_names()
                 
    fao_heads <- fao %>%
        filter(unit %in% c("Head", "1000 Head")) %>%
        drop_na(value) %>%
        mutate(value = ifelse(unit == "1000 Head", value * 1000, value)) %>%
        mutate(unit = "Head")
    
    # get country iso3c
    fao_countries <- unique(fao$area)
    country_code_lookup <- suppressWarnings(countrycode::countrycode(fao_countries,
                                                                     origin = "country.name",
                                                                     destination = "iso3c"))
    names(country_code_lookup) <- fao_countries
    #country_code_lookup[is.na(country_code_lookup)]
    country_code_lookup[grepl("d'Ivoire", names(country_code_lookup))] <- "CIV"
    
    fao_heads %>%
        mutate(country_iso3c = country_code_lookup[area]) %>%
        drop_na(country_iso3c) %>% 
        mutate(taxa = recode(item, 
                             "Asses" = "equidae",
                             "Camels" = "camelidae",
                             "Cattle" = "cattle" ,
                             "Chickens"  = "birds",
                             "Goats" = "goats",
                             "Horses" = "equidae",
                             "Mules"  = "equidae",
                             "Sheep"  = "sheep",
                             "Cattle and Buffaloes" = "cattle", # ? fair?
                             "Poultry Birds" = "birds",
                             "Sheep and Goats" = "sheep/goats",
                             "Buffaloes"  = "buffaloes",
                             "Ducks"  = "birds",
                             "Geese and guinea fowls" = "birds",
                             "Pigs"  = "swine",
                             "Turkeys"  = "birds",
                             "Rabbits and hares" = "hares/rabbits",
                             "Camelids, other" = "camelidae",
                             "Rodents, other" = NA_character_,
                             "Pigeons, other birds" = "birds"
                             )) %>% 
        drop_na(taxa)  %>% 
        group_by(country_iso3c, year, taxa) %>% 
        summarize(population = sum(value)) %>% 
        ungroup()
}
