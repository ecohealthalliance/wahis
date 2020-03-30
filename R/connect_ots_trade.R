#' Download ots data
#' @import dplyr here tradestatistics
#' @importFrom readr write_rds
#' @export
download_trade <- function(){
  
  product_ids <- full_join(tradestatistics::ots_products, tradestatistics::ots_communities) %>%
    filter(community_name %in% c("Foodstuffs","Animal and Vegetable Bi-Products", "Vegetable Products", "Animal Hides",  "Animal Products")) %>%
    pull(product_code)
  
  suppressWarnings(dir.create(here("data-raw")))
  ots <- ots_create_tidy_data(years = c(2000:2017), table = "yrpc",  products = product_ids,
                              include_communities = TRUE, include_shortnames = TRUE) 
  
  write_rds(ots, here("data-raw/ots-trade.rds"))
}

#' Transform ots data to return pairwise trade values
#' @import dplyr tidyr here
#' @importFrom readr read_rds
#' @export
transform_trade <- function(){
  
  ots <- read_rds(here("data-raw/ots-trade.rds"))
  
  # assign country destination and origin
  ots_export <- ots %>%
    select(year, product_code, country_origin = reporter_iso, country_destination = partner_iso, value = export_value_usd)
  
  ots_import <- ots %>%
    select(year, product_code, country_origin = partner_iso, country_destination = reporter_iso, value = import_value_usd)
  
  # for trades reported more than once, assume max value
  ots_bilateral_long <- bind_rows(ots_export, ots_import) %>%
    drop_na(value) %>%
    filter(country_origin != country_destination) %>%
    group_by(year, product_code, country_origin, country_destination) %>%
    filter(value == max(value)) %>% 
    distinct() %>%
    ungroup() %>%
    mutate_at(.vars = c("country_origin", "country_destination"), ~toupper(.))
  
  # reshape
  ots_bilateral_wide <- ots_bilateral_long %>%
    mutate(product_code = paste0("trade_dollars_", product_code)) %>%
    pivot_wider(names_from = product_code, values_from = value) 
  
  return(ots_bilateral_wide)
  
}