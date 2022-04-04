#' Download wto tourism data
#' @param directory to save tourism data
#' @param username UNWTO email
#' @param password UNWTO password
#' @import dplyr here xml2 purrr stringr
#' @importFrom rvest html_session html_form set_values submit_form jump_to
#' @export
download_tourism <- function(username, password, directory){
  
  suppressWarnings(dir.create(here(directory, "wto-tourism")))
  
  # sign into site
  start_page <- "https://www.e-unwto.org/action/showLogin?uri=%2Faction%2FdoSearch%3FstartPage%3D0%26ConceptID%3D2451%26target%3Dtopic%26pageSize%3D100"
  session <- html_session(start_page)
  form <- html_form(read_html(start_page))[[4]]
  filled_form <- set_values(form,
                            login = username,
                            password =  password)
  submit_form(session, filled_form)
  
  # Arrivals of non-resident tourists at national borders, by country of residence 
  page <- read_html("https://www.e-unwto.org/action/doSearch?startPage=0&ConceptID=2451&target=topic&pageSize=100")
  
  country_urls <- xml_find_all(page, xpath = "//a[@class='ref nowrap']") %>% 
    xml_attrs() %>%
    map(~paste0("https://www.e-unwto.org", .x[["href"]]))
  
  # download data for each country
  walk(country_urls, function(url){
    filename <- basename(url) %>% str_extract('[0-9]+')
    filename_full <- paste0(url, "/suppl_file/", filename, ".xlsx")
    xlsx_download <- jump_to(session, filename_full)
    writeBin(xlsx_download$response$content, here(directory, "wto-tourism", paste0(filename, ".xlsx")))
  })
}

#' Transform wto tourism data
#' @param directory where tourism is saved
#' @import dplyr here stringr purrr
#' @importFrom readxl read_xlsx
#' @importFrom countrycode countrycode
#' @importFrom janitor clean_names
#' @export
transform_tourism <- function(directory){
  
  filenames <- list.files(here::here(directory, "wto-tourism"),
                          pattern = "*.xlsx",
                          full.names = TRUE)  
  
  wto_bilateral <- map_df(filenames, function(file){
    
    # read in file
    wto <- try(suppressMessages(readxl::read_xlsx(file)), silent = TRUE)
    if(class(wto)[1]=="try-error"){return()}
    
    # get country name
    country <- wto %>% select(1) %>% slice(3) %>% pull()
    country_iso3c <-  countrycode(country, origin = "country.name", destination = "iso3c")
    
    # get year published
    pub_year <- names(wto)[1] %>% str_sub(-5, -2)
    
    # transform
    wto %>% 
      purrr::set_names(.[5,]) %>%
      janitor::clean_names() %>%
      slice(-c(1:6)) %>%
      select(-2, -starts_with("market"), -starts_with("percent")) %>%
      rename(country_origin = na) %>%
      mutate(country_origin = suppressWarnings(countrycode(country_origin, origin = "country.name", destination = "iso3c"))) %>%
      drop_na(country_origin) %>%
      mutate(country_destination = country_iso3c) %>%
      mutate_at(vars(starts_with("x")), ~na_if(., ".")) %>%
      mutate_at(vars(starts_with("x")), ~as.numeric(.)) %>%
      pivot_longer(cols = -c(country_origin, country_destination), names_to = "year", values_to = "n_tourists") %>%
      mutate(year = str_remove(year, "x")) %>%
      drop_na(n_tourists) %>% 
      mutate(pub_year = pub_year)
  })
  
  # for dupes, select more recent published date, then higher value (countries can have two values when both have the same country code)
  out <- wto_bilateral %>%
    group_by(country_origin, country_destination, year) %>%
    filter(pub_year == max(pub_year)) %>%
    filter(n_tourists == max(n_tourists)) %>%
    distinct() %>%
    ungroup() %>%
    select(-pub_year)
  
  return(out)
}
