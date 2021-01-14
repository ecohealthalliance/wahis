#' Get country GDPs from World Bank API
#' @import dplyr tidyr purrr
#' @importFrom jsonlite fromJSON
#' @export
#' @example 
#' get_wb_indicators(
#'   indicators_list = 
#'     list(gdp_dollars = "NY.GDP.MKTP.CD",
#'          human_population = "SP.POP.TOTL")
#' )
get_wb_indicators <- function(indicators_list){
    inds <- purrr::imap(indicators_list, function(code, name) {
        ind <- jsonlite::fromJSON(paste0("http://api.worldbank.org/v2/country/all/indicator/", code, "?per_page=20000&format=json"))
        ind <- ind[[2]] %>%
            dplyr::select(country_iso3c = countryiso3code, year = date, value) %>% 
            tibble::as_tibble() %>% 
            dplyr::filter(country_iso3c != "") %>%
            tidyr::drop_na() %>%
            dplyr::mutate(year = as.integer(year))
        names(ind)[3] <- name
        return(ind)
    })
    
    Reduce(
        function(x, y) {
            dplyr::full_join(x, y, by = c("country_iso3c", "year"))
        },
        inds,
        tibble::tibble(country_iso3c = character(0), year = integer(0))
    )
}

