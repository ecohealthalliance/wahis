% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/transform_outbreak_reports.R
\name{transform_outbreak_reports}
\alias{transform_outbreak_reports}
\title{Convert a list of scraped outbreak reports into formatted data}
\usage{
transform_outbreak_reports(outbreak_reports, report_list)
}
\arguments{
\item{outbreak_reports}{a list of outbreak report content produced by ingest_report()}

\item{report_list}{produced by scrape_outbreak_report_list()}
}
\value{
A list of three tibbles
}
\description{
Yields three tibbles (saved in a single list object): outbreak_reports_events (high level outbreak data), outbreak_reports_outbreaks (detailed location and impact data for outbreak events), outbreak_reports_diseases_unmatched (diseases from the outbreak reports that did not match the ANDO ontology. These diseases are not removed from the database.)
}
