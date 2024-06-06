library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(safepaths)

# ---- Configure LAN Paths and DB Connection -----
lan < - safepaths::get_network_path()
source(glue("{lan}/development/sql/git-to-source/credential-preprocessing.R"))

# set connection string to decimal
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

source("./sql/credential-preprocessing.R")

# ---- Run Queries -----
dbGetQuery(con, qry00a_check_null_epens)
dbGetQuery(con, qry00b_check_unique_epens)
dbGetQuery(con, qry00c_CreateIDinSTPCredential)
dbGetQuery(con, qry00d_SetPKeyinSTPCredential)
dbGetQuery(con, qry01_ExtractAllID_into_STP_Credential_Record_Type)

# ---- Exclude Record Type 1  ----
dbGetQuery(con, qry02a_Record_With_PEN_Or_STUID)
dbGetQuery(con, qry02b_Drop_No_PEN_Or_No_STUID)
dbGetQuery(con, qry02c_Update_Drop_No_PEN_or_No_STUID)

# ---- Exclude Record Type 2  ----
dbGetQuery(con, qry03a_Drop_Record_Developmental)
dbGetQuery(con, qry03b_Update_Drop_Record_Developmental)

dbGetQuery(con, qry03c_create_table_EnrolmentSkillsBasedCourse)
dbGetQuery(con, qry03c_Drop_Skills_Based)
dbGetQuery(con, qry03d_create_table_Suspect_Skills_Based)
dbGetQuery(con, qry03d_Update_Drop_Record_Skills_Based)
dbGetQuery(con, qry03e_Find_Suspect_Skills_Based)
dbGetQuery(con, qry03e_Keep_Skills_Based)
dbGetQuery(con, qry03f_Update_Keep_Record_Skills_Based)
dbGetQuery(con, qry03f_Update_Suspect_Skills_Based)
dbGetQuery(con, qry03g_create_table_SkillsBasedCourses)
dbGetQuery(con, qry03g_Drop_Developmental_Credential_CIPS)
dbGetQuery(con, qry03h_Update_Developmental_CIPs)
dbGetQuery(con, qry03i_Drop_RecommendationForCert)
dbGetQuery(con, qry03j_Update_RecommendationForCert)
dbGetQuery(con, qry03k_Drop_Developmental_CIPS)
dbGetQuery(con, qry03k_Update_ID_for_Drop_Dev_Credential_CIP)

dbGetQuery(con, qry04_Update_RecordStatus_Not_Dropped)

