#' Scrape CIA factbook for country borders
#' @import here stringr dplyr tidyr
#' @importFrom xml2 read_html 
#' @importFrom rvest html_table
#' @importFrom janitor clean_names 
#' @importFrom purrr map 
#' @export
get_country_borders <- function(){
  page <- read_html("https://www.cia.gov/library/publications/the-world-factbook/fields/281.html")
  borders <- html_table(page)[[1]] %>%
    as_tibble() %>%
    janitor::clean_names() %>%
    # remove countries that do not have borders
    mutate(land_boundaries = ifelse(str_detect(land_boundaries, "border countries"), land_boundaries, NA)) %>%
    drop_na(land_boundaries) %>% 
    # remove note section
    mutate(land_boundaries = str_split(land_boundaries, "note:")) %>% 
    mutate(land_boundaries = map(land_boundaries, ~.[[1]])) %>%
    # extract section with border countries
    mutate(land_boundaries = str_extract(land_boundaries, ":[^:]+$")) %>%
    # remove spaces and numbers and measurement units
    mutate(land_boundaries = str_remove(land_boundaries, ":\n        ")) %>%
    mutate(land_boundaries = str_remove_all(land_boundaries, "[0-9]+")) %>%
    mutate(land_boundaries = str_remove_all(land_boundaries, " km")) %>%
    mutate(land_boundaries = str_remove_all(land_boundaries, " \\(.*\\)|\\.")) %>%
    # separate rows for each bording country
    separate_rows(land_boundaries, sep = ",") %>%
    mutate(land_boundaries = trimws(land_boundaries)) 
  
  borders_bilateral <- borders %>%
    mutate_all(~suppressWarnings(countrycode(., "country.name", "iso3c"))) %>%
    drop_na() %>%
    set_names(c("country_origin", "country_destination")) 
  
  borders_bilateral <-  bind_rows(borders_bilateral, 
                                  borders_bilateral %>% 
                                    rename(country_origin = country_destination,
                                           country_destination = country_origin)) %>%
    distinct() %>%
    mutate(shared_border = TRUE)
  
  return(borders_bilateral)
  
}

#' calculate country centroid distance
#' @import dplyr tidyr CoordinateCleaner
#' @importFrom janitor clean_names
#' @importFrom geosphere distm 
#' @export
get_country_distance <- function(){
  
  country_controids <- CoordinateCleaner::countryref %>%
    as_tibble() %>%
    janitor::clean_names() %>%
    filter(type == "country") %>%
    select(iso3c = iso3, centroid_lon, centroid_lat) %>%
    mutate(iso3c = as.character(iso3c)) %>%
    group_by(iso3c) %>%
    summarize(centroid_lon = mean(centroid_lon), centroid_lat = mean(centroid_lat)) %>%
    ungroup()

  country_controids_bilateral <- tibble(iso3c_1 = country_controids$iso3c, iso3c_2 = country_controids$iso3c) %>%
    expand(iso3c_1, iso3c_2) %>%
    filter(iso3c_1 != iso3c_2) %>%
    left_join(country_controids, by=c("iso3c_1" = "iso3c")) %>%
    left_join(country_controids, by=c("iso3c_2" = "iso3c"), suffix = c("_1", "_2")) %>%
    rowwise() %>%
    mutate(gc_dist = geosphere::distm(c(centroid_lon_1, centroid_lat_1), c(centroid_lon_2, centroid_lat_2), fun = distGeo)) %>%
    select(country_origin = iso3c_1, country_destination = iso3c_2, gc_dist)
  
  return(country_controids_bilateral)
}
