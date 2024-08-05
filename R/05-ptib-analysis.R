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
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Private_Institutions_Credentials_Raw"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')))

# Part 1 ----
## ---- Add PSSM_Credential to PTIB data ----
dbExecute(decimal_con, qry_Private_Credentials_00a_Append)

## ---- Check CIP length ----
dbGetQuery(decimal_con, qry_Private_Credentials_00b_Check_CIP_Length)

## ---- Remove periods from CIPs ----
dbExecute(decimal_con, qry_Private_Credentials_00c_Clean_CIP_Period)
dbGetQuery(decimal_con, qry_Private_Credentials_00b_Check_CIP_Length) # sanity check

## ---- Check CIPs against infoware 6digit CIPs ----
dbGetQuery(decimal_con, qry_Private_Credentials_00d_Check_CIPs)


## ---- Update Exclude column ----
# Flag not for credit and ESL programs and unclassified 99.9999
dbExecute(decimal_con, "ALTER TABLE T_Private_Institutions_Credentials 
                ADD  Exclude VARCHAR(255), LCIP_NAME VARCHAR(255)")

dbExecute(decimal_con, "UPDATE T_Private_Institutions_Credentials 
           SET T_Private_Institutions_Credentials.LCIP_NAME = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_NAME
           FROM T_Private_Institutions_Credentials 
           INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016
           ON T_Private_Institutions_Credentials.LCIP_CD = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD")

dbExecute(decimal_con, "UPDATE T_Private_Institutions_Credentials 
           SET T_Private_Institutions_Credentials.Exclude = 1
           WHERE (((T_Private_Institutions_Credentials.LCIP_NAME) ='English as a second language') OR
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
dbExecute(decimal_con, "DELETE FROM T_Private_Institutions_Credentials
                WHERE intYear <> 'Avg 2017 & 2018'")

# Part 2 ----
## STOP !!! Update model year in queries ----

## ---- Count domestic grads ----
dbExecute(decimal_con, qry_Private_Credentials_01a_Domestic)

## ---- Count domestic and international grads ----
dbExecute(decimal_con, qry_Private_Credentials_01b_Domestic_International)

## ---- Compute percent of domestic and international grads that are domestic ----
dbExecute(decimal_con, qry_Private_Credentials_01c_Percent_Domestic)

## ---- Compute unknown or blank immigration status ----
dbExecute(decimal_con, qry_Private_Credentials_01d_Grads_Blank)

## ---- Join domestic and blank ----
dbExecute(decimal_con, qry_Private_Credentials_01e_Grads_Union)

## ---- Sum of union query ----
dbExecute(decimal_con, qry_Private_Credentials_01f_Grads)

## ---- Summarize the Grads by Credential/Age ----
dbExecute(decimal_con, qry_Private_Credentials_05i_Grads)

## ---- Delete PTIB rows from Graduate_Projections ----
dbExecute(decimal_con, qry_Private_Credentials_05i0_Grads_by_Year_Delete)

## ---- Update Graduate_Projections ----
dbExecute(decimal_con, qry_Private_Credentials_05i1_Grads_by_Year)

## ---- Delete excess age groups ----
dbExecute(decimal_con, qry_Private_Credentials_05i2_Delete_AgeGrps)

dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01a_Domestic")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01b_Domestic_International")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01c_Percent_Domestic")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01d_Grads_Blank")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01e_Grads_Union")

# Part 3 ----
## ---- Counts grads by CIP ----
dbExecute(decimal_con, qry_Private_Credentials_06b_Cohort_Dist)

## ---- Sums total by age ----
dbExecute(decimal_con, qry_Private_Credentials_06c_Cohort_Dist_Total)

## ---- Delete PTIB rows from Cohort_Program_Distributions_Projected ----
dbExecute(decimal_con, qry_Private_Credentials_06d0_Cohort_Dist_Delete_Projected)

## ---- Delete PTIB rows from Cohort_Program_Distributions_Static ----
dbExecute(decimal_con, qry_Private_Credentials_06d0_Cohort_Dist_Delete_Static)

## ---- Update Cohort_Program_Distributions_Projected ----
dbExecute(decimal_con, qry_Private_Credentials_06d1_Cohort_Dist_Projected)

## ---- Update Cohort_Program_Distributions_Static ----
dbExecute(decimal_con, qry_Private_Credentials_06d1_Cohort_Dist_Static)

dbExecute(decimal_con, qry_Private_Credentials_06d2_Projected_Delete_AgeGrps)
dbExecute(decimal_con, qry_Private_Credentials_06d2_Static_Delete_AgeGrps)

# Clean up ----
## ---- Drop tmp qry datasets ----
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01f_Grads")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_05i_Grads")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_06b_Cohort_Dist")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_06c_Cohort_Dist_Total")

## ---- Drop Main Datasets
dbExecute(decimal_con, "DROP TABLE T_Private_Institutions_Credentials")
dbExecute(decimal_con, "DROP TABLE T_Private_Institutions_Credentials_Clean")

dbExecute(decimal_con, "DROP TABLE Graduate_Projections")
dbExecute(decimal_con, "DROP TABLE Cohort_Program_Distributions_Static")
dbExecute(decimal_con, "DROP TABLE Cohort_Program_Distributions_Projected")

## ---- Drop Lookups
dbExecute(decimal_con, "DROP TABLE T_PSSM_Credential_Grouping")
dbExecute(decimal_con, "DROP TABLE T_PTIB_Y1_to_Y10")
dbExecute(decimal_con, "DROP TABLE INFOWARE_L_CIP_6DIGITS_CIP2016")

## ---- disconnect_connect ----
dbDisconnect(decimal_con)
