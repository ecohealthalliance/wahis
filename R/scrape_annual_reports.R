#' Support function for scrape_annual_report_list
#' @param url response 
#' @import xml2 purrr dplyr tidyr stringi
#' @noRd
report_rab <- function(resp) {
    if (is.null(resp)) return(tibble())
    code = stri_extract_last_regex(resp$url,"\\w{3}$")
    if (resp$status_code != 200 || is.null(resp$status_code)) return(tibble(code = code, status_code = resp$status_code))
    html <- read_html(resp$content, encoding = "ISO-8859-1")
    html_tb <- html %>%
        xml_nodes(xpath = '//table[@class="Table27"]/*') %>%
        as_list()
    
    out <- map_dfr(html_tb[-1], function(tr) {
        tibble(
            code = code,
            status_code = resp$status_code,
            year = as.integer(stri_trim_both(tr[[1]][[1]])),
            `1` = names(tr[[3]])[2] == "a",
            `2` = names(tr[[5]])[2] == "a",
            `0` = names(tr[[7]])[2] == "a"
        )
    })
    if (nrow(out) == 0) {
        return(tibble(code = code))
    } else {
        return(out)
    }
}

#' Extract list of WAHIS Annual and Semi-Annual Reports
#' @return A tibble with annual report status by country
#' @export
#' @import xml2 purrr dplyr tidyr stringr
#' @importFrom scrapetools map_curl
scrape_annual_report_list <- function() {
    
    message("Getting list of countries")
    base_page <- read_html("http://www.oie.int/wahis_2/public/wahid.php/Countryinformation/Countryhome")
    
    countries <- base_page %>%
        xml_nodes(xpath = '//*[@id="country6"]/option') %>%
        xml_attrs() %>%
        map(~list(country=.["label"], code = .["value"])) %>%
        transpose() %>%
        as_tibble() %>%
        unnest(cols = c(country, code)) %>%
        filter(code != "0")
    
    country_calls <- countries %>%
        mutate(form = map(code, ~list(this_country_code = ., detailed="1")),
               url = paste0("https://www.oie.int/wahis_2/public/wahid.php/Countryinformation/reporting/reporthistory?", code)) %>%
        sample_frac(1)
    
    message("Fetching list of annual reports for each country asynchronously")
    country_resps <- map_curl(
        url = country_calls$url,
        .handle_form = country_calls$form,
        .host_con = 6L,
        .timeout = 20*60L,
        .handle_opts = list(low_speed_limit = 100, low_speed_time = 30),
        .retry = 3
    )
    
    resp_tab <- map_dfr(country_resps, report_rab)
    
    misses <- filter(resp_tab, is.na(year)) %>% pull(code)
    misses <- c(misses, stri_extract_last_regex(country_resps %>% keep(is.null) %>% names,"\\w{3}$"))
    if (length(misses)) {
        message("Fetching countries that failed on first round: ", paste(misses, collapse = ", "))
        miss_calls <- filter(country_calls, code %in% misses)
        retry_country_resps <- map_curl(
            url = miss_calls$url,
            .handle_form = miss_calls$form,
            .host_con = 1L,
            .delay = 1,
            .timeout = 20*60L,
            .handle_opts = list(low_speed_limit = 100, low_speed_time = 30)
        )
        retry_resp_tab <- map_dfr(retry_country_resps, report_rab)
        all_tab <- bind_rows(resp_tab, retry_resp_tab)
    } else {
        message("No fetching failures on first round")
        all_tab <- resp_tab
    }
    
    all_tab <- all_tab %>%
        filter(!is.na(year))
    all_tab <- left_join(countries, all_tab, by = "code") %>%
        complete(country, year) %>%
        arrange(country, year) %>%
        mutate(report_year = as.character(year)) %>% 
        select(country, code, report_year, status_code, `1`, `2`, `0`) %>%
        pivot_longer(cols = matches("(1|2|0)"), names_to = "semester", values_to = "reported") %>%
        mutate(datettime_checked_reported = Sys.time())
    
    return(all_tab)
    
}