#' Download fao taxa population data
#' @import here
#' @export
download_taxa_population <- function(){
    suppressWarnings(dir.create(here("data-raw/fao-taxa-population")))
    download.file("http://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Livestock_E_All_Data_(Normalized).zip",
                  destfile =  here("data-raw/fao-taxa-population/fao-taxa-population.zip"))
}

