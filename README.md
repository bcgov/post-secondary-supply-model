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

Scripts are labeled sequentially and run in that order with a few exceptions (WIP).  Each analysis script has a corresponding script that 
handles loading of the data required for analysis.  Currently the model has only been tested data load scripts run prior to analysis, although future work may change this.  The run order is:

- 01a-enrolment-preprocessing.R 
- 01b-credential-preprocessing.R 
- 01c-credential-analysis.R 
01d-enrolment-analysis.R 
02a-appso-programs.R 
02a-bgs-program-matching.R 
02a-dacso-program-matching.R 
02a-update-cred-non-dup.R 
02b-1-pssm-cohorts.R 
02b-2-pssm-cohorts-new-labour-supply.R 
02b-3-pssm-cohorts-occupation-distributions.R 
03-near-completers-ttrain.R 
04-graduate-projections.R 
05-ptib-analysis.R 
06-program-projections.R 
07-occupation-projections.R 



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
