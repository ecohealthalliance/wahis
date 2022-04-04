
#' Download UN migrant data
#' @param directory to save downloaded migration data
#' @import here
#' @export
download_human_migration <- function(directory){
  suppressWarnings(dir.create(here(directory, "un-human-migration")))
  download.file("https://www.un.org/en/development/desa/population/migration/data/empirical2/data/UN_MigFlow_All_CountryFiles.zip",
                destfile =  here(directory, "un-human-migration/migrant-flow.zip"))
  unzip(here(directory, "un-human-migration/migrant-flow.zip"), exdir = here(directory, "un-human-migration"))
}

#' Transform UN migrant data
#' @param directory where migration data is saved
#' @import here dplyr tidyr purrr stringr
#' @importFrom countrycode countrycode
#' @importFrom readxl read_xlsx
#' @importFrom janitor clean_names
#' @export

transform_human_migration <- function(directory){
  
  # all data
  filenames <- list.files(here::here(directory, "un-human-migration"),
                          pattern = "*.xlsx",
                          full.names = TRUE)
  
  
  un <-  map_df(filenames, function(file){
    
    # get country name from file
    country <- basename(file) %>%
      stringr::str_remove(".xlsx")
    
    if(country == "Republic of Moldova") {country <- "Rep of Moldova"}
    if(country == "Russian Federation") {country <- "Russian Fed"}

    country_iso3c <- countrycode::countrycode(country, origin = "country.name", destination = "iso3c")
    
    sheets <- readxl::excel_sheets(file)
    sheet_pref1 <- paste(country, "by Residence")
    sheet_pref2 <- paste(country, "by Citizenship")
    sheet <- ifelse(sheet_pref1 %in% sheets, sheet_pref1, sheet_pref2)
    if(country == "TfYR of Macedonia") {sheet <- "TfYR of Macedonia by Res"}
    if(country == "United States of America") {sheet <- "USA by Place of birth"}
    
    # read data
    migrant <- readxl::read_xlsx(file, sheet = sheet, skip = 20) %>%
      janitor::clean_names() %>%
      filter(!od_name %in% c("Total", "Unknown")) %>%
      mutate(od_name = countrycode::countrycode(od_name,  origin = "country.name", destination = "iso3c")) %>%
      select(-coverage, -area, -area_name, -reg, -reg_name, -dev, -dev_name) %>%
      mutate_all(~as.character(.))
    
    # get directions for emigrants and immigrants
    migrant_emigrate <- migrant %>%
      filter(type == "Emigrants") %>%
      mutate(country_origin = country_iso3c) %>%
      rename(country_destination = od_name) %>%
      select(-type)
    
    migrant_immigrate <- migrant %>%
      filter(type == "Immigrants") %>%
      mutate(country_destination = country_iso3c) %>%
      rename(country_origin = od_name)%>%
      select(-type)
    
    # transform
    migrant_long <- bind_rows(migrant_emigrate, migrant_immigrate) %>%
      pivot_longer(names_to = "year", values_to = "n_human_migrants", cols = -c("country_origin", "country_destination")) %>%
      mutate(year = str_remove(year, "x")) %>%
      filter(!n_human_migrants %in%  c("-", "..", "D"),
             year >= 2000)
    
  }) 
  
  # select max reported
  un <- un %>%
    filter(country_origin !=  country_destination) %>%
    mutate(n_human_migrants = as.numeric(n_human_migrants)) %>%
    group_by(country_destination, country_origin, year) %>%
    filter(n_human_migrants == max(n_human_migrants)) %>%
    slice(1) %>%
    ungroup

  
  return(un)
  
}
