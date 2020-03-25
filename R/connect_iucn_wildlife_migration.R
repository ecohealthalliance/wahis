#' Download iucn data
#' @import dplyr readr here
#' @importFrom purrr map_df
#' @importFrom jsonlite fromJSON
#' @export

download_wildlife <- function(){
  token <- Sys.getenv("IUCN_REDLIST_KEY")
  countries <- fromJSON(paste0("https://apiv3.iucnredlist.org/api/v3/country/list?token=", token))$results
  
  wildlife <- map_df(countries$isocode, function(iso){
   
     result <- fromJSON(paste0("https://apiv3.iucnredlist.org/api/v3/country/getspecies/", iso, "?token=", token))$result 
    
    if(length(result)){
      result <-  result %>% mutate(country = iso)
    }else{
      result <- NULL
    }
    return(result)
 })
  
  suppressWarnings(dir.create(here("data-raw")))
  
  write_rds(wildlife, here("data-raw/iucn-wildlife.rds"))
}



#' Transform iucn data to return counts of overlapping species between pairwise countries
#' @import dplyr tidyr here countrycode rvest taxadb
#' @importFrom purrr map imap_dfr
#' @export

transform_wildlife_migration <- function(){
  
  # get list of migratory species
  page <- read_html("http://groms.de/groms_neu/view/order_stat_patt_spanish.php?search_pattern=")
  groms <- html_table(page)[[2]] %>%
    slice(-1) %>%
    pull(1)
    
  # get list of bird species
  suppressMessages(td_create("col"))
  aves <- suppressMessages(filter_rank(name = "Aves", rank = "class")) %>%
    pull(scientificName)
  
  # read in iucn data and filter for migratory species and remove bird species
  wildlife <- read_rds(here("data-raw/iucn-wildlife.rds")) %>%
    select(scientific_name, country) %>%
    distinct() %>%
    filter(country != "DT") %>% # disputed territory
    filter(scientific_name %in% groms) %>%
    filter(!scientific_name %in% aves) %>%
    mutate(country = countrycode(country, origin = "iso2c", destination = "iso3c")) 

  wildlife_grp <- wildlife %>%
    group_by(country) 
  
  wildlife_list <- group_split(wildlife_grp) %>%
    set_names(pull(group_keys(wildlife_grp), country))

  # get all possible combinations of countries and look up intersection of species
  combos <- combn(x = unique(wildlife$country), m = 2,
                  simplify = FALSE, FUN = sort)
  combo_names <- map(combos, ~paste(., collapse = "-"))
  
  wildlife_intersects <- map(combos, function(x){
    intersect(wildlife_list[[x[1]]]$scientific_name, wildlife_list[[x[2]]]$scientific_name)
  }) %>% set_names(combo_names)
  
  # generate tibble of number animals shared by countries
  wildlife_intersects_count <- imap_dfr(wildlife_intersects, ~tibble(countries = .y, n_migratrory_wildlife = length(.x))) %>%
    separate(countries, into = c("country_origin", "country_destination"), sep = "-") 
  
  # because data is non-directional, copy the data for the opposite direction
  wildlife_intersects_count <- wildlife_intersects_count %>%
    bind_rows(., wildlife_intersects_count %>%
                rename(country_destination = country_origin, country_origin = country_destination))
  
  return(wildlife_intersects_count)
  
}
