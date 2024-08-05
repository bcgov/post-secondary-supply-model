# ******************************************************************************
# Private Training Institutions Branch (PTIB)
# 
# Required Tables
#   PTIB_Credentials
#   T_PSSM_Credential_Grouping
#   INFOWARE_L_CIP_6DIGITS_CIP2016
#   Graduate_Projections
#   Cohort_Program_Distributions_Projected
#   Cohort_Program_Distributions_Static
#   T_PTIB_Y1_to_Y10
#
# Part 1: Clean PTIB data
# * Update age groups, CIPs
# * Add and update exclude column
#
# Part 2: Domestic graduates
#
# Part 3: Cohort distributions
#
# ******************************************************************************

library(RODBC)
library(arrow)
library(tidyverse)
library(odbc)
library(RJDBC) ## loads DBI

# Setup
# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to database ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Required data tables and SQL ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Private_Institutions_Credentials_Imported_2021_03"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."PTIB_Credentials"'))) 
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')))

# ---- Load R versions ----
pssm_cred_grps <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping"')))
ptib_initial <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Private_Institutions_Credentials_Imported_2021-03"')))
grad_proj <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."Graduate_Projections_Ref"')))
cpd_proj <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected_Ref"')))
cpd_static <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static_Ref"')))


# Part 1 ----
## ---- Add PSSM_Credential to PTIB data ----
dbGetQuery(decimal_con, qry_Private_Credentials_00a_Append)

## ---- Check CIP length ----
dbGetQuery(decimal_con, qry_Private_Credentials_00b_Check_CIP_Length)

## ---- Remove periods from CIPs ----
dbGetQuery(decimal_con, qry_Private_Credentials_00c_Clean_CIP_Period)

## ---- Check CIPs against infoware 6digit CIPs ----
dbGetQuery(decimal_con, qry_Private_Credentials_00d_Check_CIPs)

## ---- Update Exclude column ----
# Excluded not for credit and ESL programs and unclassified 99.9999 manually with “Exclude=1”
# Added SQL code to update these exclusions automatically
dbGetQuery(decimal_con, "ALTER TABLE T_Private_Institutions_Credentials 
                ADD  
                Exclude VARCHAR(255),
                LCIP_NAME VARCHAR(255)")

dbGetQuery(decimal_con, "UPDATE T_Private_Institutions_Credentials 
           SET
            T_Private_Institutions_Credentials.LCIP_NAME = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_NAME
           FROM T_Private_Institutions_Credentials INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016
           ON T_Private_Institutions_Credentials.LCIP_CD = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD")

dbExecute(decimal_con, "UPDATE T_Private_Institutions_Credentials 
           SET T_Private_Institutions_Credentials.Exclude = 1
           WHERE (((T_Private_Institutions_Credentials.LCIP_NAME) ='English as a sedecimal_cond language') OR
          ((T_Private_Institutions_Credentials.LCIP_NAME) LIKE '%not for credit%') OR
          ((T_Private_Institutions_Credentials.LCIP_CD)='999999') );")

dbGetQuery(decimal_con, "ALTER TABLE T_Private_Institutions_Credentials
                DROP COLUMN LCIP_NAME;")

## ---- Update age groups ----
dbGetQuery(decimal_con, qry_Private_Credentials_00f_Recode_Age_Group)

## ---- Fix immigration status ----
# Immigration_Status “#N/A” recoded to “(blank)”
# none for 2019-20; instead there was “Unknown”
T_Private_Institutions_Credentials %>% 
  count(Immigration_Status)

## ---- Copy to Clean table ----
# Copied as T_Private_Institutions_Credentials_Clean
# there may be duplicate CIPs after the cleaning but that is ok as next step sums and divides by 2 (the number of years) for the average
dbExecute(decimal_con, "SELECT *
                INTO T_Private_Institutions_Credentials_Clean
                FROM T_Private_Institutions_Credentials;")


## ---- Age averages ----
## not sure if this will be necessary if only one year in data???
dbGetQuery(decimal_con, "ALTER TABLE T_Private_Institutions_Credentials
                ALTER COLUMN intYear VARCHAR(255);")

dbGetQuery(decimal_con, qry_Private_Credentials_00g_Avg)

# I updated the above query as it didn't run as-is, but 2017, 2018, and Avg were all in the resulting table
# I think table should actually ONLY result in table with just Avg; remove the 2017, 2018 individual rows
dbGetQuery(decimal_con, "DELETE FROM T_Private_Institutions_Credentials
                WHERE intYear <> 'Avg 2017 & 2018'")

# Part 2 ----
## STOP !!! Update model year in queries ----

## ---- Count domestic grads ----
dbGetQuery(decimal_con, qry_Private_Credentials_01a_Domestic)

## ---- Count domestic and international grads ----
### note to self: this qry still has #N/A which was changed to (blank) 
### none this time, but could affect these queries ?
dbGetQuery(decimal_con, qry_Private_Credentials_01b_Domestic_International)

## ---- Compute percent of domestic and international grads that are domestic ----
dbGetQuery(decimal_con, qry_Private_Credentials_01c_Percent_Domestic)

## ---- Compute unknown or blank immigration status ----
## computes Blank/Unknown immigration status records to include as domestic grads; 
## 2019-09-06 updated criteria to “(blank) Or Unknown”
dbGetQuery(decimal_con, qry_Private_Credentials_01d_Grads_Blank)

## ---- Join domestic and blank ----
dbGetQuery(decimal_con, qry_Private_Credentials_01e_Grads_Union)

## ---- Sum of union query ----
dbGetQuery(decimal_con, qry_Private_Credentials_01f_Grads)

## ---- Summarize the Grads by Credential/Age ----
dbGetQuery(decimal_con, qry_Private_Credentials_05i_Grads)

## ---- Delete PTIB rows from Graduate_Projections ----
dbExecute(decimal_con, qry_Private_Credentials_05i0_Grads_by_Year_Delete)

Graduate_Projections <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"')))

Graduate_Projections <- Graduate_Projections %>% 
  filter(Survey!="PTIB")

## ---- Update Graduate_Projections ----
## adds grads for all years to Graduate_Projections
dbGetQuery(decimal_con, qry_Private_Credentials_05i1_Grads_by_Year)

T_PTIB_Y1_to_Y10 <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PTIB_Y1_to_Y10"')))

## ---- Delete excess age groups ----
## ADDED 2024 Replacement for manually deleting excess age groups 
## Looks like, blanks, unknowns, 16 or less and 65+ were not in final table
dbGetQuery(decimal_con, qry_Private_Credentials_05i2_Delete_AgeGrps)


# Part 3 ----
## ---- TBD ----
dbExecute(decimal_con, qry_Private_Credentials_06b_Cohort_Dist)
## ---- TBD ----
dbExecute(decimal_con, qry_Private_Credentials_06c_Cohort_Dist_Total)
## ---- Delete PTIB rows from Cohort_Program_Distributions_Projected ----
dbExecute(decimal_con, qry_Private_Credentials_06d0_Cohort_Dist_Delete_Projected)

## ---- Delete PTIB rows from Cohort_Program_Distributions_Static ----
dbExecute(decimal_con, qry_Private_Credentials_06d0_Cohort_Dist_Delete_Static)

## ---- Update Cohort_Program_Distributions_Projected ----
dbExecute(decimal_con, qry_Private_Credentials_06d1_Cohort_Dist_Projected)

## ---- Update Cohort_Program_Distributions_Static ----
dbExecute(decimal_con, qry_Private_Credentials_06d1_Cohort_Dist_Static)


# Clean up ----
## ---- Drop helper qry datasets ----
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01a_Domestic")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01b_Domestic_International")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01c_Percent_Domestic")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01d_Grads_Blank")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01e_Grads_Union")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01f_Grads")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_05i_Grads")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_06b_Cohort_Dist")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_06c_Cohort_Dist_Total")

## ---- disconnect_connect ----
dbDisconnect(decimal_con)
