#' Convert a list of scraped ourbreak reports to a list of table
#' @param outbreak_reports a list of outbreak reports produced by [ingest_outbreak_report]
#' @import dplyr tidyr
#' @importFrom purrr keep map map_dfr map_lgl imodify reduce
#' @importFrom janitor clean_names
#' @export

transform_outbreak_reports <- function(outbreak_reports) {
    # wahis <- readr::read_rds(here::here("data-processed", "processed-outbreak-reports.rds"))
    
    # Remove report errors ---------------------------------------------------
    outbreak_reports2 <- keep(outbreak_reports, function(x){
        !is.null(x) && !is.null(x$report_status) && x$report_status == "available"
    })
    
    if(!length(outbreak_reports2)) return(NULL)
    
    # Summary table ---------------------------------------------------
    exclude_fields <- c("Related reports", "outbreaks", "diagnostic_tests" )
    
    outbreak_summary <- map_dfr(outbreak_reports2, function(x){
        
        x$related_reports <- paste(x$related_reports, collapse = "; ")
        
        exclude_index <- which(names(x) %in% exclude_fields)
        as_tibble(x[-exclude_index])
        
    }) %>% 
        janitor::clean_names() 
    
    # Thread ID in summary table ---------------------------------------------------
    outbreak_summary <- outbreak_summary %>%
        mutate(related_reports = ifelse(related_reports=="", id, related_reports))
    
    # older reports might are updated with newer related reports
    outbreak_summary <- outbreak_summary %>%
        group_by(related_reports) %>%
        mutate(thread = group_indices()) %>%
        ungroup()
    
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
