#' Support function to format messy dates into  ISO-8601
#' @param field Vector of dates formatted as "dd/mm/yyyy". Incomplete dates (e.g., "mm/yyyy" are accepted).
#' @import dplyr tidyr stringr
#' @importFrom lubridate dmy myd ymd
#' @noRd

messy_dates <- function(field){
    
    out <- tibble(date_col = field) %>% 
        mutate(date_col = na_if(date_col, "-")) %>% 
        mutate(date_col = na_if(date_col, "0000")) %>% 
        mutate(date_col = na_if(date_col, "")) %>% 
        mutate(date_nchar = str_length(date_col))
    
    if(8 %in% unique(out$date_nchar)) {
        dates_8 <- out %>% 
            filter(date_nchar == 8) %>% 
            pull(date_col)
        if(all(str_detect(dates_8, "'"))){
            out <- out %>% 
                mutate(date_col = if_else(date_nchar == 8, str_remove_all(date_col, "'"), date_col)) %>% 
                mutate(date_col = if_else(date_nchar == 8, paste(substr(date_col, 5, 6), substr(date_col, 1, 4), sep = "/"), date_col)) %>% 
                mutate(date_nchar = str_length(date_col))
        }
    }
    
    if(any(!unique(out$date_nchar) %in% c(4, 7, 10, NA_integer_))){
        warning("Date format not recognized")
    }
    
    #TODO add more checks on ranges of month, day, year
    
    out <- out %>% 
        mutate(date_col = case_when(
            date_nchar == 4 ~ suppressWarnings(ymd(date_col, truncated = 2)),
            date_nchar == 7 ~ suppressWarnings(myd(date_col, truncated = 1)),
            date_nchar == 10 ~ suppressWarnings(dmy(date_col)))) 

    return(out$date_col)
}
