
#' Download Animal Disease Ontology from AgroPortal to specified destination
#' @param destination_folder folder to save ontology as csv
#' @export
#' @import dplyr readr
#' @importFrom janitor clean_names
#' @importFrom fs file_delete

destination_folder <- here::here("inst/diseases")

get_disease_ontology <- function(destination_folder){
    
    tmp_destination_file <- paste0(destination_folder, "/ando_tmp.csv.gz")
    
    download.file("http://data.agroportal.lirmm.fr/ontologies/ANDO/download?apikey=1de0a270-29c5-4dda-b043-7c3580628cd5&download_format=csv", destfile = tmp_destination_file)
    
    ando <- read_csv(tmp_destination_file)
    ando <- ando %>% 
        select("Class ID", "Preferred Label", "Synonyms", "Obsolete", "Parents" ) %>% 
        janitor::clean_names()
    
    distination_file <- paste0(destination_folder, "/ando_ontology.csv")
    write_csv(ando, distination_file)
    message(paste("ANDO ontology downloaded to", distination_file))
    fs::file_delete(tmp_destination_file)
}

