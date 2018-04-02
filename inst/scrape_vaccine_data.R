# A little script for scraping vaccine data from WAHIS.  Will form core of 
# future mass scraping functions down the line.

library(httr)
library(rvest)
library(xml2)
library(tidyverse)

# Pretend we are a browser
my_headers = c(Origin="http://www.oie.int",
               `Upgrade-Insecure-Requests`="1",
               `Content-Type`="application/x-www-form-urlencoded",
               `User-Agent`="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
               `Accept`="text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
               `DNT`="1",
               `Referer`="http://www.oie.int/wahis_2/public/wahid.php/Countryinformation/Vaccination",
               `Accept-Encoding`="gzip, deflate",
               `Accept-Language`="en-US,en;q=0.9")

# Set a new handle. This resets cookies, etc.
h1 <- handle('')

# Send an initial request, selecting anthrax as disease
req_1 <- POST(url = "http://www.oie.int/wahis_2/public/wahid.php/Countryinformation/Vaccination",
              body=list(disease=17),
              encode="form",
              add_headers(.headers=my_headers),
              handle=h1)

# Extract all countries
countries <- content(req_1, encoding="windows-1252") %>%
    html_nodes(xpath='//select[@id="country6"]/option') %>% 
    xml_attr("value") %>% 
    `[`(-1)
# Set years of interest
years <- 2005:2017

#For testing
countries <- countries[1:3]
years <- years[1:3]

# Create data structure
vacdata <- data_frame()

# Loop over countries
for (cou in countries) {
    
    # Submit form with country data
    req_2 <- POST(url = "http://www.oie.int/wahis_2/public/wahid.php/Countryinformation/Vaccination",
                  body=paste0("country_select_method=1&locn1=6&country1=", cou, "&country2=", cou, "&country3=", cou, 
                              "&country4", cou, "&country5=", cou, "&country6=",
                              cou, "&country6=", cou, "&multipleselectregion=-1&totalregion=7&locn2=", cou, "&add=country"),
                  encode = "raw",
                  add_headers(.headers=my_headers),
                  handle=h1)
    
    # Loop over years
    for (yr in years) {
        #Progress monitoring
        cat(cou, "\t", yr, "\r")  
        #Submit form changing year
        req_3 <- POST(url = "http://www.oie.int/wahis_2/public/wahid.php/Countryinformation/Vaccination",
                      body=list(year=yr, selected_species=0), encode = "form",
                      add_headers(.headers=my_headers),
                      handle=h1)
        
        #Check if there is data first
        out_node <- content(req_3,  encoding="windows-1252") %>% 
            html_node(xpath="//table[contains(@class,'table-pad0-100')]") 
        if(class(out_node) != "xml_missing") {
            
            #Both append to data strucutre and write to disk
            newdata <- html_table(out_node, fill=TRUE, header=TRUE) %>% 
                mutate_all(as.character) %>% 
                mutate(Year = yr)
            vacdata <- bind_rows(vacdata, newdata)
            write_csv(newdata, "vacdata.csv", append=TRUE)
        }
    }
}