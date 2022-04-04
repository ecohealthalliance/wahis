

This package provides access to veterinary disease data from [OIE WAHIS](https://wahis.oie.int/#/home) as well as several other data sets that may support analysis of veterinary diseases.

#### wahis data

- _outbreak reports_ 
   - `inst/processing_scripts/process_outbreak_reports.R` provides an example workflow for retrieving wahis outbreak reports
   - `scrape_outbreak_report_list()` returns a master list of available outbreak reports
   - `ingest_report()` accepts a curl response from the wahis API and returns outbreak report content as a list. `safe_ingest()` is a wrapper to handle errors when ingesting reports.
   - `transform_outbreak_reports()` converts outbreak report content into formatted data. It yields three tibbles (saved in a single list object):
      - `outbreak_reports_events` contains high-level outbreak event data. Each row corresponds to a report. `report_id` is the unique report ID and `url_report_id` is the value appended to the API url for the given report.
      - `outbreak_reports_outbreaks` provides detailed location and impact data for outbreak events. This table can be joined with `outbreak_reports_events` by `report_id`. `outbreak_location_id` is a unique ID for each location (e.g, farm or village) within a outbreak.
      - `outbreak_reports_diseases_unmatched` lists diseases from the outbreak reports that did not match the ANDO ontology. These diseases are _not_ removed from the database.
      
- _six month reports_ 
   - `inst/processing_scripts/process_six_month_reports.R` provides an example workflow for retrieving wahis 6 mognth reports
   - `scrape_six_month_report_list()` returns a master list of available 6 month reports
   - `ingest_report()` accepts a curl response from the wahis API and returns 6 month report content as a list. `safe_ingest()` is a wrapper to handle errors when ingesting reports.
   - `transform_six_month_reports()` converts six month report content into formatted data. It yields three tibbles (saved in a single list object):
      - `six_month_reports_summary` contains high-level six-month data. It provides disease status (present/absent/unreported) and case counts by country, disease, taxa. It also includes control measures.
      - `six_month_reports_detail` provides case data at finer temportal and/or spatial resolutions, as available. 
      - `six_month_reports_diseases_unmatched` lists diseases from the six month reports that did not match the ANDO ontology. These diseases are _not_ removed from the database.

#### other data sets

The following data sets can be accessed through this package:  
- Birdlife data for migratory bird species (`download_bird_migration()` and `transform_bird_migration()`)  
- FAO livestock population (`download_livestock()` and `transform_livestock()`)  
- Country borders from CIA factbook (`get_country_borders()`)  
- UN human migration data (`download_human_migration()` and `transform_human_migration()`)  
- Wildlife migration data combined from IUCN and GROMS (`download_wildlife()` and `transform_wildlife_migration()`)  
- OTS trade data (`download_trade()` and `transform_trade()`)  
- WTO tourism data (`download_tourism()` and `transform_tourism()`)  
- FAO taxa population data (`download_taxa_population()` and `transform_taxa_population()`)  
- Raster data of livestock and human populations, climatic variables, economic activity and transit accessibility (`download_rasters()` and `transform_rasters()`)  


