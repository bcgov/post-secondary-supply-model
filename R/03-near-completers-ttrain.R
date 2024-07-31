# Notes: watch for Age_Grouping variable, documentation mentions having removed it from earlier queries and linked later.  not sure what this means.
# also, need to update T-Year_Survey_Year as is a dependency in DACSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded.  The pattern to update is obvious from prior
# year's entries, but some rationale would be helpful.

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

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

# ---- Read raw data ----
source(glue::glue("{lan}/development/sql/gh-source/03-near-completers-ttrain/near-completers-investigation-ttrain.R"))
source(glue::glue("{lan}/development/sql/gh-source/03-near-completers-ttrain/dacso-near-completers.R"))
dbExistsTable(decimal_con, "t_dacso_data_part_1")
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Age"')))

# ---- Execute SQL ----
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD BTHDT_CLEANED NVARCHAR(20) NULL")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD ENDDT_CLEANED NVARCHAR(20) NULL")
dbExecute(decimal_con, qry_make_tmp_table_Age_step2)
dbExecute(decimal_con, "UPDATE tmp_tbl_Age_AppendNewYears SET ENDDT_CLEANED = '' WHERE ENDDT_CLEANED = '00/1/0000'")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD BTHDT_DATE NVARCHAR(20) NULL")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD ENDDT_DATE NVARCHAR(20) NULL")
dbExecute(decimal_con, qry_make_tmp_table_Age_step3)
dbExecute(decimal_con, "UPDATE tmp_tbl_Age_AppendNewYears SET ENDDT_DATE = NULL WHERE ENDDT_DATE = '1900-01-01'")
dbExecute(decimal_con, qry_make_tmp_table_Age_step4)

dbExecute(decimal_con, "ALTER TABLE T_DACSO_Data_Part_1 ADD Age_At_Grad FLOAT NULL")
dbExecute(decimal_con, qry99a_Update_Age_At_Grad)
dbExecute(decimal_con, "ALTER TABLE T_DACSO_Data_Part_1 ADD Grad_Status_Factoring_in_STP nvarchar(2) NULL")
dbExecute(decimal_con, qry99a_Update_Age_At_Grad)

# Note: possibly want to edit this to include only age groups up to age 64 
dbExecute(decimal_con, qry_make_T_DACSO_DATA_Part_1_TempSelection)

# note from Ian: Copy and paste the results (of the following query) into the Excel workbook for analysis 
dbGetQuery(decimal_con, qry99_Investigate_Near_Completes_vs_Graduates_by_Year) 
dbExecute(decimal_con, qry01_Match_DACSO_to_STP_Credential_Non_DUP_on_PEN)
dbExecute(decimal_con, qry_Update_STP_PRGM_Credential_Awarded_Name )

# How many PEN matched records also match STP on credential category
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_credential nvarchar(10) NULL")
dbExecute(decimal_con, qry02_Match_DACSO_STP_Credential_PSI_CRED_Category)

# How many PEN matched records also match STP on CIP4
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_cip_code_4 nvarchar(10) NULL")
dbExecute(decimal_con, qry03_Match_DACSO_STP_Credential_CIPCODE4)

# How many PEN matched records also match STP on CIP2
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD Match_CIP_CODE_2 nvarchar(10) NULL")
dbExecute(decimal_con, qry03b_Match_DACSO_STP_Credential_CIPCODE2)

# How many PEN matched records also match STP on Award Year. 
#Add the new year combinations required in the query design first 
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_award_school_year nvarchar(10) NULL")
dbExecute(decimal_con, qry04_Match_DACSO_STP_Credential_AwardYear)

# How many PEN matched records also match STP on Inst code
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_inst nvarchar(10) NULL")
dbExecute(decimal_con, qry05_Match_DACSO_STP_Credential_Inst)

# Print summary of the matching results.
dbGetQuery(decimal_con, qry06_Match_DACSO_STP_Credential_Summary)

#  These are considered final matches to STP credential.
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD final_consider_a_match nvarchar(10) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_all_4_flag nvarchar(10) NULL")
dbExecute(decimal_con, qry07_DACSO_STP_Credential_MatchAll4_Flag)

#  flags the records that match on inst, award year, credential, and CIP 2 (but not CIP 4) as final matches too. 
dbExecute(decimal_con, qry08_DACSO_STP_Credential_Final_Match_Flag)

dbExecute(decimal_con, )

# ---- Clean Up ----
dbDisconnect(decimal_con)
dbExecute(decimal_con, "DROP TABLE DACSO_Matching_STP_Credential_PEN")
dbExecute(decimal_con, "DROP TABLE stp_dacso_prgm_credential_lookup")
dbExecute(decimal_con, "DROP TABLE T_DACSO_DATA_Part_1_TempSelection")


