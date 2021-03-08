#' Scrape birdlife data for migratory bird species by country
#' @param directory where bird migration data is saved
#' @import purrr stringr 
#' @importFrom httr GET write_disk
#' @import here
#' @export
download_bird_migration <- function(directory){
  suppressWarnings(dir.create(here(directory, "bli-bird-migration")))
  walk(1:280, function(i){
    url <- paste0("http://datazone.birdlife.org/species/results?cty=", i, "&cri=&fam=0&gen=0&spc=&cmn=&bt=&rec=N&vag=N&sea=&wat=&aze=&lab=&enb=&stmig=Y")
    ipad <- str_pad(i, 3, pad = "0")
    request <- httr::GET(url, 
                         httr::write_disk(here(paste0(directory, "/bli-bird-migration/", ipad, ".html")), overwrite = TRUE))
  })
}


#' Transform birdlife data to return counts of overlapping migratory species between pairwise countries
#' @param directory where bird migration data is saved
#' @import dplyr tidyr purrr xml2 stringr here
#' @importFrom rvest html_nodes
#' @importFrom countrycode countrycode
#' @importFrom assertthat are_equal
#' @export
transform_bird_migration <- function(directory){
  
  # import data
  filenames <- list.files(here(directory, "bli-bird-migration"),
                          pattern = "*.html",
                          full.names = TRUE)
  
  bird_pages <-  map(filenames, function(file){
    page <- try(suppressWarnings(read_xml(file, encoding = "ISO-8859-1", as_html = TRUE, options = c("RECOVER", "NOERROR", "NOBLANKS"))), silent = TRUE)
    if(class(page)[1]=="try-error"){return()}
    return(page)
  }) %>% compact()
  
  # remove error pages
  errors <- purrr::map_lgl(bird_pages, function(page){
    html_nodes(page, "body") %>% 
      xml_text() %>%
      str_detect("looks like something went wrong")
  })
  bird_pages <- bird_pages[!errors]
  
  # get country names from xml
  country <- purrr::map_chr(bird_pages, function(page){
    xml_find_all(page, xpath="/html/body/div[2]/div/div/div/div/div[1]/div/div/div[1]") %>%
      xml_text()  %>%
      str_remove(., "\r\n                Search terms\r\n                                    Country/Territory = ") %>%
      str_remove(., "\r\n                                    Species Types = Migratory\r\n                                    Order by Taxonomic Sequence")
  })
  
  # lookup country iso3c
  country_iso3c <- suppressWarnings(countrycode(country, origin = "country.name", destination = "iso3c"))
  keep <- which(!is.na(country_iso3c))
  bird_pages <- bird_pages[keep]
  country_iso3c <- country_iso3c[keep]
  
  # get migratory birds in each country
  birds <- map(bird_pages, function(page){
    xml_find_all(page, xpath="//tr[@class='qpq-table-tr']") %>%
      xml_text() %>%
      str_extract(., "[^\r\n]+")
  }) %>%
    set_names(country_iso3c) %>%
    compact()
  
  country_iso3c <- names(birds) # countries with data
  
  #MANUAL FIX there are 4 russias <- combine them
  country_iso3c[duplicated(country_iso3c)]
  rus_index <- which(names(birds)=="RUS")
  rus_birds <- birds[rus_index] %>% reduce(c) %>% unique()
  birds[rus_index] <- NULL
  birds <- compact(birds)
  birds$RUS <- rus_birds
  
  country_iso3c <- names(birds) # unique countries
  assertthat::are_equal(length(birds), n_distinct(country_iso3c))
  
  # get all possible combinations of countries and look up intersection of species
  combos <- combn(x = country_iso3c, m = 2,
                  simplify = FALSE, FUN = sort)
  combo_names <- map(combos, ~paste(., collapse = "-"))
  
  bird_intersects <- map(combos, function(x){
    intersect(birds[[x[1]]], birds[[x[2]]])
  }) %>% set_names(combo_names)
  
  # generate tibble of number migratory birds shared by countries
  bird_intersects_count <- imap_dfr(bird_intersects, ~tibble(countries = .y, n_migratory_birds = length(.x))) %>%
    separate(countries, into = c("country_origin", "country_destination"), sep = "-") %>% 
    filter(country_origin != country_destination)
  
  # because data is non-directional, copy the data for the opposite direction
  bird_intersects_count <- bird_intersects_count %>%
    bind_rows(., bird_intersects_count %>%
                rename(country_destination = country_origin, country_origin = country_destination))
  
  assertthat::are_equal(nrow(janitor::get_dupes(bird_intersects_count, country_origin, country_destination)), 0)
  
  return(bird_intersects_count)
}
