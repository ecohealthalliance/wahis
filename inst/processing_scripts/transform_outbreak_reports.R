library(tidyverse)

# Load files -------------------------------------------------------------
wahis <- readr::read_rds(here::here("data-processed", "processed-outbreak-reports.rds"))

# Remove report errors ---------------------------------------------------
wahis <- discard(wahis, function(x){
    length(x) == 1
})

# Summary table ---------------------------------------------------
outbreak_summary <- map_dfr(wahis, function(x){
    
    x$related_reports <- paste(x$related_reports, collapse = "; ")
    
    exclude_fields <- c("Related reports", "outbreaks", "diagnostic_tests" )
    exclude_index <- which(names(x) %in% exclude_fields)
    as_tibble(x[-exclude_index])
    
}) %>% 
    janitor::clean_names() 

# Thread ID in summary table ---------------------------------------------------
outbreak_summary <- outbreak_summary %>%
    mutate(related_reports = ifelse(related_reports=="", id, related_reports))

# method 1 - assumes that older reports might not be updated with newer related reports
# outbreak_summary$thread <- NA
# thr <- 1
# for(i in 1:nrow(outbreak_summary)){
#     
#     related_reports <- str_split(outbreak_summary$related_reports[i], "; ") %>% unlist()
#     
#     if(any(!is.na(outbreak_summary$thread[outbreak_summary$id %in% related_reports]))){
#         outbreak_summary$thread[outbreak_summary$id %in% related_reports] <- unique(na.omit(outbreak_summary$thread[outbreak_summary$id %in% related_reports]))
#     }else{
#         outbreak_summary$thread[outbreak_summary$id %in% related_reports] <- thr
#         thr <- thr + 1
#     }
# }

# method 2 - assumes that older reports might are updated with newer related reports
outbreak_summary <- outbreak_summary %>%
    group_by(related_reports) %>%
    mutate(thread = group_indices()) %>%
    ungroup()


# Outbreak detail ---------------------------------------------------
tictoc::tic()
outbreak_detail <- map_dfr(wahis, function(x){
    
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
tictoc::toc()

# Diagnostic table ---------------------------------------------------
outbreak_diagnostics <- map_dfr(wahis, function(x){
    
    tests <- x$diagnostic_tests 
    
    if(is.null(dim(tests))){
        return(tibble(id = x$id))
    }
    
    tests <- tests %>% mutate(id = x$id)
    return(tests)
})


# R profile

# Export -----------------------------------------------
wahis_joined <- list(outbreak_summary = outbreak_summary, outbreak_detail = outbreak_detail, outbreak_diagnostics = outbreak_diagnostics)
fs::dir_create(here::here("data-processed", "db"))
iwalk(wahis_joined, ~write_csv(.x, here::here("data-processed", "db", paste0("outbreak_reports_", .y, ".csv.xz"))))

write_rds(wahis_joined, here::here("data-processed", "outbreak-reports-data.rds"), compress = "xz", compression = 9L)
