#' Convert a list of scraped ourbreak reports to a list of table
#' @param outbreak_reports a list of outbreak reports produced by [ingest_outbreak_report]
#' @import dplyr tidyr
#' @importFrom purrr keep map map_dfr map_lgl imodify reduce
#' @importFrom janitor clean_names
#' @importFrom stringr str_detect str_extract
#' @export

transform_outbreak_reports <- function(outbreak_reports) {

    # Remove report errors ---------------------------------------------------
    outbreak_reports2 <- keep(outbreak_reports, function(x){
        !is.null(x) && !is.null(x$report_status) && x$report_status == "available"
    })
    
    if(!length(outbreak_reports2)) return(NULL)
    
    # Summary table ---------------------------------------------------
    exclude_fields <- c("Related reports", "outbreaks", "diagnostic_tests", "related_reports" )
    
    outbreak_summary <- map_dfr(outbreak_reports2, function(x){
        exclude_index <- which(names(x) %in% exclude_fields)
        as_tibble(x[-exclude_index])
    }) %>% 
        janitor::clean_names() 
    
    # Immediate report threads ---------------------------------------------------
    outbreak_summary <- outbreak_summary %>%
        mutate(immediate_report = ifelse(str_detect(report_type, "Immediate notification"), id, immediate_report)) %>%
        mutate(follow_up = ifelse(str_detect(report_type, "Immediate notification"), 0, str_extract(report_type, "[[:digit:]]+"))) %>%
        mutate(final_report = str_detect(report_type, "Final report"))

    #TODO QA check outbreak_summary %>% filter(is.na(immediate_report))
    
    # Outbreak detail ---------------------------------------------------
    outbreak_detail <- map_dfr(outbreak_reports2, function(x){
        
        outbreaks <- x$outbreaks
        
        map_dfr(outbreaks, function(y){
            
            if(length(y) == 1 && y == "There are no new outbreaks in this report"){
                return(tibble(outbreak_location = outbreaks, id = x$id))
            } 
            
            this_outbreak <- y[map_lgl(y, ~is.list(.))] 
            this_outbreak <- imodify(this_outbreak, ~mutate(.x, desc = .y)) %>%
                reduce(bind_rows) %>%
                mutate(id = x$id)
            
            outbreak_desc <- y[map_lgl(y, ~!is.list(.))] %>%
                as_tibble() %>%
                mutate(outbreak = colnames(.)[1]) %>%
                rename(outbreak_location = 1)
            
            this_outbreak <- crossing(outbreak_desc, this_outbreak)
            
            return(this_outbreak)
        })
    })
    
    # Diagnostic table ---------------------------------------------------
    outbreak_diagnostics <- map_dfr(outbreak_reports2, function(x){
        
        tests <- x$diagnostic_tests 
        
        if(is.null(dim(tests))){
            return(tibble(id = x$id))
        }
        
        tests <- tests %>% mutate(id = x$id)
        return(tests)
    })
    
    
    # Export -----------------------------------------------
    wahis_joined <- list(outbreak_summary = outbreak_summary, outbreak_detail = outbreak_detail, outbreak_diagnostics = outbreak_diagnostics)
    return(wahis_joined)
    
}
