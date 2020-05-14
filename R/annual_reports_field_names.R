#' Get list of annual report field names
#' @noRd

annual_reports_field_names <- function(){
    
    list(submission_info = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'submission_info', 'submission_value', 'submission_animal_type'),
         diseases_present = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'disease', 'occurrence', 'serotype_s', 'new_outbreaks', 'total_outbreaks', 'species', 'control_measures', 'official_vaccination', 'measuring_units', 'susceptible', 'cases', 'deaths', 'killed_and_disposed_of', 'slaughtered', 'vaccination_in_response_to_the_outbreak_s', 'oie_listed', 'notes'),
         diseases_absent = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'taxa', 'disease', 'date_of_last_occurrence', 'species', 'control_measures', 'official_vaccination', 'oie_listed', 'notes'),
         diseases_present_detail = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'period', 'adm', 'serotype_s', 'new_outbreaks', 'total_outbreaks', 'species', 'family_name', 'latin_name', 'measuring_units', 'susceptible', 'cases', 'deaths', 'killed_and_disposed_of', 'slaughtered', 'vaccination_in_response_to_the_outbreak_s', 'adm_type', 'temporal_scale', 'disease'),
         diseases_unreported = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'taxa', 'disease', 'oie_listed'),
         disease_humans = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'disease', 'no_information_available', 'disease_absent', 'disease_present_number_of_cases_unknown', 'disease_present_number_of_cases_known', 'human_cases', 'human_deaths'),
         animal_population = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'species', 'production', 'total', 'units', 'number', 'units_2'),
         veterinarians = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'veterinarian_field', 'public_sector', 'total', 'private_sector', 'veterinarian_class'),
         national_reference_laboratories = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'name', 'contacts', 'latitude', 'longitude'),
         national_reference_laboratories_detail = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'laboratory', 'disease', 'test_type'),
         vaccine_manufacturers = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'manufacturer', 'contacts', 'year_of_start_of_activity', 'year_of_cessation_of_activity'),
         vaccine_manufacturers_detail = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'disease', 'manufacturer', 'vaccine', 'vaccine_type', 'year_of_start_of_production', 'year_of_end_of_production_if_production_ended'),
         vaccine_production = c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'manufacturer', 'vaccine', 'doses_produced', 'doses_exported'))
    
}
