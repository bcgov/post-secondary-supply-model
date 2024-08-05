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

source(glue::glue("{lan}/development/sql/gh-source/05-ptib-analysis/05-private-training-institutions-sql.R"))

# ---- Required data tables and SQL ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Private_Institutions_Credentials_Imported_2021_03"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."PTIB_Credentials"'))) 
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')))

# Part 1 ----
## ---- Add PSSM_Credential to PTIB data ----
dbExecute(decimal_con, qry_Private_Credentials_00a_Append)
dbReadTable(decimal_con, "") %>% View()

## ---- Check CIP length ----
dbGetQuery(decimal_con, qry_Private_Credentials_00b_Check_CIP_Length)
dbReadTable(decimal_con, "") %>% View()

## ---- Remove periods from CIPs ----
dbExecute(decimal_con, qry_Private_Credentials_00c_Clean_CIP_Period)
dbReadTable(decimal_con, "") %>% View()

## ---- Check CIPs against infoware 6digit CIPs ----
dbGetQuery(decimal_con, qry_Private_Credentials_00d_Check_CIPs)
dbReadTable(decimal_con, "") %>% View()

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

dbExecute(decimal_con, "ALTER TABLE T_Private_Institutions_Credentials
                DROP COLUMN LCIP_NAME;")

## ---- Update age groups ----
dbExecute(decimal_con, qry_Private_Credentials_00f_Recode_Age_Group)

## ---- Fix immigration status ----
# what to do with unknowns? see documentation

## ---- Copy to Clean table ----
dbExecute(decimal_con, "SELECT *
                INTO T_Private_Institutions_Credentials_Clean
                FROM T_Private_Institutions_Credentials;")


## ---- Age averages ----
dbExecute(decimal_con, "ALTER TABLE T_Private_Institutions_Credentials
                ALTER COLUMN intYear VARCHAR(255);")

dbExecute(decimal_con, qry_Private_Credentials_00g_Avg)
dbReadTable(decimal_con, "") %>% View()

# I updated the above query as it didn't run as-is, but 2017, 2018, and Avg were all in the resulting table
# I think table should actually ONLY result in table with just Avg; remove the 2017, 2018 individual rows
dbExecute(decimal_con, "DELETE FROM T_Private_Institutions_Credentials
                WHERE intYear <> 'Avg 2017 & 2018'")

# Part 2 ----
## STOP !!! Update model year in queries ----

## ---- Count domestic grads ----
dbExecute(decimal_con, qry_Private_Credentials_01a_Domestic)
dbReadTable(decimal_con, "qry_Private_Credentials_01a_Domestic") %>% View()

## ---- Count domestic and international grads ----
### note to self: this qry still has #N/A which was changed to (blank) 
### none this time, but could affect these queries ?
dbExecute(decimal_con, qry_Private_Credentials_01b_Domestic_International)
dbReadTable(decimal_con, "qry_Private_Credentials_01b_Domestic_International") %>% View()

## ---- Compute percent of domestic and international grads that are domestic ----
dbExecute(decimal_con, qry_Private_Credentials_01c_Percent_Domestic)
dbReadTable(decimal_con, "") %>% View()

## ---- Compute unknown or blank immigration status ----
## computes Blank/Unknown immigration status records to include as domestic grads; 
## 2019-09-06 updated criteria to “(blank) Or Unknown”
dbExecute(decimal_con, qry_Private_Credentials_01d_Grads_Blank)
dbReadTable(decimal_con, "") %>% View()

## ---- Join domestic and blank ----
dbExecute(decimal_con, qry_Private_Credentials_01e_Grads_Union)
dbReadTable(decimal_con, "") %>% View()

## ---- Sum of union query ----
dbExecute(decimal_con, qry_Private_Credentials_01f_Grads)
dbReadTable(decimal_con, "") %>% View()

## ---- Summarize the Grads by Credential/Age ----
dbExecute(decimal_con, qry_Private_Credentials_05i_Grads)
dbReadTable(decimal_con, "") %>% View()

## ---- Delete PTIB rows from Graduate_Projections ----
dbExecute(decimal_con, qry_Private_Credentials_05i0_Grads_by_Year_Delete)
dbReadTable(decimal_con, "") %>% View()

## ---- Update Graduate_Projections ----
## adds grads for all years to Graduate_Projections
dbExecute(decimal_con, qry_Private_Credentials_05i1_Grads_by_Year)
dbReadTable(decimal_con, "") %>% View()

Graduate_Projections <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"')))

Graduate_Projections <- Graduate_Projections %>% 
  filter(SURVEY!="PTIB")

## ---- Delete excess age groups ----
dbExecute(decimal_con, qry_Private_Credentials_05i2_Delete_AgeGrps)

dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01a_Domestic")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01b_Domestic_International")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01c_Percent_Domestic")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01d_Grads_Blank")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01e_Grads_Union")

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
## ---- Drop tmp qry datasets ----
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01f_Grads")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_05i_Grads")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_06b_Cohort_Dist")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_06c_Cohort_Dist_Total")

## ---- Drop Main Datasets
dbExecute(decimal_con, "DROP TABLE T_Private_Institutions_Credentials")
dbExecute(decimal_con, "DROP TABLE T_Private_Institutions_Credentials_Clean")
dbExecute(decimal_con, "DROP TABLE PTIB_Credentials")
dbExecute(decimal_con, "DROP TABLE T_Private_Institutions_Credentials_Imported_2021-03")

dbExecute(decimal_con, "DROP TABLE Graduate_Projections")
dbExecute(decimal_con, "DROP TABLE Cohort_Program_Distributions_Static")
dbExecute(decimal_con, "DROP TABLE Cohort_Program_Distributions_Projected")

## ---- Drop Lookups
dbExecute(decimal_con, "DROP TABLE T_PSSM_Credential_Grouping")
dbExecute(decimal_con, "DROP TABLE T_PTIB_Y1_to_Y10")
dbExecute(decimal_con, "DROP TABLE INFOWARE_L_CIP_6DIGITS_CIP2016")

## ---- disconnect_connect ----
dbDisconnect(decimal_con)
