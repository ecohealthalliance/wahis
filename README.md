
# wahis

This repo is for processing and eventually serving data from the [OIE WAHIS](http://www.oie.int/wahis_2/public/wahid.php) database as an R package.

- `inst/scraping_scripts` contains scripts to download HTML and PDF-formatted
   records from OIE.  These are downloaded to `data-raw`.  As this is a very
   slow process and these raw data are biggist, these data are `.gitignored`,
   and can by synced with an EHA S3 bucket with `inst/sync_aws_data.R`
   
-  Functions (In `R/`) process individual HTML or PDF files.  There are three
   types of records - outbreaks, old outbreak PDFs, and annual reports. There
   should be one function for each type of file that processes it into a record 
   that can be inserted into a database.  These should have tests, developed
   as we encounter various pathological records.
   
-  `inst/processing_scripts` contains scripts to run on all downloaded data,
   using the package functions, to generate the final database.
   
-  Final form for all the data should be a series of CSVs that also work as
   an SQL database (probably MonetDB or duckdb, possibly with JSON extensions)

-  Package structure will eventually be similar to **lemis** or **citesdb**,
   but with the database also archived/served remotely for other processes
   in the REPEL project.
   
-  CI/CD should eventually scrape WAHIS, rebuild and reploy the database
   regularly (monthly) or with updates to the package.


