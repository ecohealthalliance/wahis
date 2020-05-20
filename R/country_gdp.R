#' Get country GDPs from World Bank API
#' @import dplyr tidyr
#' @importFrom jsonlite fromJSON
#' @export
get_country_gdp <- function(){
    gdp <- fromJSON(paste0("http://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?per_page=20000&format=json"))
    gdp[[2]] %>%
        dplyr::select(country_iso3c = countryiso3code, year = date, gdp_dollars = value) %>%
        as_tibble() %>%
        filter(country_iso3c != "") %>%
        drop_na() %>%
        mutate(year = as.integer(year))
}
