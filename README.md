# post-secondary-supply-model

<!-- badges: start -->
[![Lifecycle:Experimental](https://img.shields.io/badge/Lifecycle-Experimental-339999)](https://github.com/bcgov/repomountie/blob/master/doc/lifecycle-badges.md)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
<!-- badges: end -->

### Purpose

A repository to house Post-Secondary Supply Model (PSSM) code base.

### Requirements

To connect to SQL Servers, we require a configuration file to connect securely to various required databases, and use [config](https://rstudio.github.io/config/) to read in the configuration. 

To connect securely to a database, the following snippet of code may be used:

```r
library(DBI)
library(odbc)

# get configuration from config file 
db_config <- config::get("decimal")

# connect to database
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# pull data from database 
strSQL <- "SELECT * FROM <table>.<name>"
df <- dbGetQuery(con, strSQL)

df
```
### Running the model

Scripts are labeled sequentially and generally run in that order with the exception of 01e-stp-distributions.R.  Most analysis scripts have a corresponding script that 
handles loading of the data required for analysis. The run order is:

Initial data pre-processing and post-secondary analysis:

- 01a-enrolment-preprocessing.R 
- 01b-credential-preprocessing.R 
- 01c-credential-analysis.R 
- 01d-enrolment-analysis.R 

Program matching scripts to aid in the cleanup of CIP codes:

- 02a-appso-programs.R 
- 02a-bgs-program-matching.R 
- 02a-dacso-program-matching.R 
- 02a-update-cred-non-dup.R 

Aggregate STP data for enrolment forecasting:

- 01e-stp-distributions.R

Because the model is run 3 times with varying configurations of inputs, the following scripts (and their associated data loading scripts) have all been automated with flags to allow for this process.  Make sure the following flags are set in your R session before running the scripts.  You will likely have to set the flags at the beginning of each script (WIP).

Create occupation and new labour supply weighted distributions:

- load-cohort-appso.R                         
- load-cohort-trd.R
- load-cohort-bgs.R 
- load-cohort-dacso.R  
- 02b-1-pssm-cohorts.R 
- 02b-2-pssm-cohorts-new-labour-supply.R 
- 02b-3-pssm-cohorts-occupation-distributions.R 

Calculate near completers ratio:

- load-near-completers-ttrain.R
- 03-near-completers-ttrain.R 

Run enrolment and graduate forecasting:

- load-graduate-projections.R
- 04-graduate-projections.R 

Load, clean and aggregate PTIB data;
- load-ptib.R
- 05-ptib-analysis.R 

Run program projections:

- load-program-projections.R
- 06-program-projections.R 

Run the final occupational model:

- load-occupation-projections.R
- 07-occupation-projections.R 

Create formatted Excel outputs:
- 08-create-final-reports.R


### Getting Help or Reporting an Issue

To report bugs/issues/feature requests, please file an [issue](https://github.com/bcgov/post-secondary-supply-model/issues).

### How to Contribute

If you would like to contribute, please see our [CONTRIBUTING](CONTRIBUTING.md) guidelines.

Please note that this project is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.

### License

```
Copyright 2024 Province of British Columbia

Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
```
