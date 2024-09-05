# This script creates static and projected distributions from several sources.
#  - Apprenticeship and TTRAIN distributions are derived from program cohort summaries 
#    built in workflow 2b (T_Cohorts_Recoded)
#  - Near Completers distributions by age and CIP were summarized in workflow 3, the 
#    source data is those students in the DACSO program survey cohort, who (did or did not?)
#    receive an earlier or later credential.
#  - the remainder are derived from Credential Non Dup table and tblCredential_HighestRank 
#
# At a high level, the script:
#   Adds near completers to projected and static distribution data sets (Y1)
#   Adds program cohorts to static distribution data sets (Y1)
#   Adds masters and doctorates to static distribution data sets (Y1)
#   Adds apprenticeships to static and projected data sets (Y1)
#   Creates static distributions for apprenticeships and near-completers (Y2-12) 
#   Creates projected distributions for apprenticeships and near-completers (Y2-12), holding Y2-12 constant.  
#   Creates projected distributions all other credentials (Y2-Y12) 
#     - uses R program written by Werner and adapted by Ian
#   
# Includes: generally age groups are 17-19, 20-24, 25-30, 30-34, 35-44, 45-54, 55-64
# Year 1: 2019/2020 
# Year 2+: 2020/2021 - 2030/2031
# Notes: Years need to be updated each model run.  Check we are projecting 12 years.  Also which age groupings 
# will we be using?
# FIXME: lookups T_APPR_Y2_to_Y10 and T_Cohort_Program_Distributions_Y2_to_Y12 ID fields aren't sequential
#        keep eyes open for impacts of this.
#        04-graduate-projections: remove space in final table name, add survey column and populate

library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

source("./sql/06-program-projections/06-program-projections.R")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
# Derived tables
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Program_Projection_Input"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Cohorts_Recoded"')))

# Rollovers from last run - we should be able to just build these up from a blank table schema
# but this is how it was done in prior years so keep for now.  Same as T_Cohorts_Recoded
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')))

# Lookups
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PSSM_Projection_Cred_Grp"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Weights_STP"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_Near_Completers"')))

# Note from documentation: update Y1 to model year and Y2_to_Y10 to years you want projected.  
# We can probably just create this table here and skip saving and uploading from year to year.
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Cohort_Program_Distributions_Y2_to_Y12"')))

# ---- survey == "PTIB" (Static and Projected) ----
dbExecute(decimal_con, 
            "INSERT INTO Cohort_Program_Distributions_Projected 
            (Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, Age_Group, [Year], [Count], Total, [Percent] )
            SELECT Survey, Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, Age_Group, [Year], [Count], Total, [Percent] 
            FROM qry_Private_Credentials_06d1_Cohort_Dist;")
dbExecute(decimal_con, 
            "INSERT INTO Cohort_Program_Distributions_Static
            ( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, Age_Group, [Year], [Count], Total, [Percent] )
            SELECT Survey, Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, Age_Group, [Year], [Count], Total, [Percent] 
            FROM qry_Private_Credentials_06d1_Cohort_Dist;")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_06d1_Cohort_Dist")

# ---- survey == 'Program_Projections_2023-2024_qry_13d' (Static and Projected) ----
# Add near completers to projected and static distribution datasets
dbExecute(decimal_con, qry_13a0_Delete_Near_Completers_Projected)
dbExecute(decimal_con, qry_13a0_Delete_Near_Completers_Static)
dbExecute(decimal_con, qry_13a_Near_completers)
dbExecute(decimal_con, qry_13b_Near_Completers_Total)
dbExecute(decimal_con, qry_13c_Near_Completers_Program_Dist)
dbExecute(decimal_con, qry_13d_Append_Near_Completers_Program_Dist_Projected_TTRAIN)
dbExecute(decimal_con, qry_13d_Append_Near_Completers_Program_Dist_Static_TTRAIN)
dbExecute(decimal_con, "drop table qry_13a_Near_completers")
dbExecute(decimal_con, "drop table qry_13b_Near_Completers_Total")
dbExecute(decimal_con, "drop table qry_13c_Near_Completers_Program_Dist")

# survey == 'Program_Projections_2023-2024_Q012e' (Static) ----
# Add program cohorts to static distribution datasets
# Note: many lcip2 creds are NULL for BACH
dbGetQuery(decimal_con, Q012a_Check_Total_for_Invalid_CIPs)
dbExecute(decimal_con, Q012b_Weight_Cohort_Dist)
dbExecute(decimal_con, Q012c_Weighted_Cohort_Dist)
dbExecute(decimal_con, Q012c1_Weighted_Cohort_Dist_TTRAIN)
dbExecute(decimal_con, Q012c2_Weighted_Cohort_Dist)
dbExecute(decimal_con, Q012c3_Weighted_Cohort_Dist_Total)
dbExecute(decimal_con, Q012c4_Weighted_Cohort_Distribution_Projected) # why create this?
dbExecute(decimal_con, Q012c5_Weighted_Cohort_Dist_TTRAIN)
dbExecute(decimal_con, Q012d_Weighted_Cohort_Dist_Total)
dbExecute(decimal_con, Q012e_Delete_Weighted_Cohort_Distribution)
dbExecute(decimal_con, Q012e_Weighted_Cohort_Distribution)
dbExecute(decimal_con, "drop table Q012b_Weight_Cohort_Dist") 
dbExecute(decimal_con, "drop table Q012c_Weighted_Cohort_Dist") 
dbExecute(decimal_con, "drop table Q012c1_Weighted_Cohort_Dist_TTRAIN") 
dbExecute(decimal_con, "drop table Q012c2_Weighted_Cohort_Dist") 
dbExecute(decimal_con, "drop table Q012c3_Weighted_Cohort_Dist_Total") 
dbExecute(decimal_con, "drop table Q012c4_Weighted_Cohort_Distribution_Projected") 
dbExecute(decimal_con, "drop table Q012c5_Weighted_Cohort_Dist_TTRAIN") 
dbExecute(decimal_con, "drop table Q012d_Weighted_Cohort_Dist_Total") 

# survey == 'Program_Projections_2023-2024_Q013e' (Static) ----
# Add masters and doctorates to static distribution datasets
# Note: lcip4_cd showing as 2D for masters and doct - cluster.  
# (same in prior model runs)
dbExecute(decimal_con, qry_12_LCP4_LCIPPC_Recode_9999)
dbGetQuery(decimal_con, Q013a_Check_PDEG_CLP_07_Only_CIP_22)
dbExecute(decimal_con, Q013b_Weight_Cohort_Dist_MAST_DOCT_Others)
dbExecute(decimal_con, Q013c_Weighted_Cohort_Dist)
dbExecute(decimal_con, Q013d_Weighted_Cohort_Dist_Total)
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Static 
          WHERE Survey LIKE 'Program_Projections_2023-2024_Q013e'") # Added
dbExecute(decimal_con, Q013e_Weighted_Cohort_Distribution)
dbExecute(decimal_con, "drop table Q013b_Weight_Cohort_Dist_MAST_DOCT_Others")
dbExecute(decimal_con, "drop table Q013c_Weighted_Cohort_Dist")
dbExecute(decimal_con, "drop table Q013d_Weighted_Cohort_Dist_Total")

# survey == 'Program_Projections_2023-2024_Q014e' (Static and Projected) ----
# adds apprenticeships to static and projected datasets
dbExecute(decimal_con, Q014b_Weighted_Cohort_Dist_APPR)
dbExecute(decimal_con, Q014c_Weighted_Cohort_Dist)
dbExecute(decimal_con, Q014d_Weighted_Cohort_Dist_Total)
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Projected 
          WHERE Survey LIKE 'Program_Projections_2023-2024_Q014e'") # Added
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Static 
          WHERE Survey LIKE 'Program_Projections_2023-2024_Q014e'") # Added
dbExecute(decimal_con, Q014e_Weighted_Cohort_Distribution_Projected)
dbExecute(decimal_con, Q014e_Weighted_Cohort_Distribution_Static )
dbExecute(decimal_con, "drop table Q014b_Weighted_Cohort_Dist_APPR")
dbExecute(decimal_con, "drop table Q014c_Weighted_Cohort_Dist")
dbExecute(decimal_con, "drop table Q014d_Weighted_Cohort_Dist_Total")

# expands static appr in graduate projections - holding counts constant
dbExecute(decimal_con, Q014f_APPSO_Grads_Y2_to_Y10)

# survey == 'Program_Projections_2023-2024_Q015e21' (Static and Projected) ----
# expands apprenticeships and near-completers to include 2020+12YR where
#  survey == Program_Projections_2023-2024_qry_13d
#  survey == Program_Projections_2023-2024_Q014e
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Projected 
          WHERE Survey LIKE 'Program_Projections_2023-2024_Q015e21'") # Run if you've been messing with iterations
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Static 
          WHERE Survey LIKE 'Program_Projections_2023-2024_Q015e22'") # Run if you've been messing with iterations
dbExecute(decimal_con, Q015e21_Append_Selected_Static_Distribution_Y2_to_Y12_Projected)
dbExecute(decimal_con, Q015e22_Append_Distribution_Y2_to_Y12_Static)

# Werner program ----
# Program takes input_data and returns output_data (write to/read from LAN below)
input_data <- dbGetQuery(decimal_con, "SELECT * FROM tbl_Program_Projection_Input") %>% 
  select(-Expr1) %>%
  complete(AgeGroup, PSI_CREDENTIAL_CATEGORY, FINAL_CIP_CODE_4, PSI_AWARD_SCHOOL_YEAR_DELAYED, fill = list(Count = 0)) %>% 
  pivot_wider(names_from = "PSI_AWARD_SCHOOL_YEAR_DELAYED", values_from = "Count") %>%
  rename("CIP" = "FINAL_CIP_CODE_4", 
         "AGE" = "AgeGroup", 
         "CRED" = "PSI_CREDENTIAL_CATEGORY") %>%
  select(CIP, CRED, AGE, 4:ncol(.)) %>%
  arrange(CIP, CRED, AGE)

write_csv(input_data, glue::glue("{lan}/development/csv/gh-source/tmp/06/input-data.csv"))

## run Werner program ----
source(glue::glue("{lan}/development/R/program projections.R")) 

output_data <- read_delim(glue::glue("{lan}/development/csv/gh-source/tmp/06/output.csv"), delim = "\t", col_names = TRUE)
names(output_data)<- paste0(2023:(2023+11), "/", 2024:(2024+11))

T_Predict_CIP_CRED_AGE <- cbind(input_data, output_data)

# pivot T_Predict_CIP_CRED_AGE from wide to long
T_Predict_CIP_CRED_AGE_Flipped <- T_Predict_CIP_CRED_AGE %>% 
  pivot_longer(-c(CIP, CRED, AGE), names_to = "Year", values_to = "Count") %>%
  filter(Year %in% c('2023/2024','2024/2025','2025/2026', '2026/2027','2027/2028',
                     '2028/2029','2029/2030','2030/2031','2031/2032', '2032/2033', 
                     '2033/2034', '2034/2035'))

dbWriteTable(decimal_con, "T_Predict_CIP_CRED_AGE_Flipped", T_Predict_CIP_CRED_AGE_Flipped)
dbGetQuery(decimal_con, qry_05_Flip_T_Predict_CIP_CRED_AGE_2_Check)

dbExecute(decimal_con, qry_09_Delete_Selected_Static_Cohort_Dist_from_Projected)

# survey == 'Program_Projections_2023-2024_qry10c' (Projected) ----
# adds projected counts to Cohort_Program_Distributions_Projected where PSSM_Credential NOT IN ('GRCT or GRDP','PDEG','MAST','DOCT') 
# (ALSO NOT IN ('APPRAPPR','APPRCERT') as these were done earlier)
dbExecute(decimal_con, qry_10a_Program_Dist_Count)
dbExecute(decimal_con, qry_10b_Program_Dist_Total)
dbExecute(decimal_con, qry_10c_Program_Dist_Distribution)
dbExecute(decimal_con, "DROP TABLE qry_10a_Program_Dist_Count")
dbExecute(decimal_con, "DROP TABLE qry_10b_Program_Dist_Total")

# survey == 'Program_Projections_2023-2024_qry12c' (Projected) ----
# adds projected counts to Cohort_Program_Distributions_Projected where PSSM_Credential IN ('GRCT or GRDP','PDEG','MAST','DOCT')
dbExecute(decimal_con, qry_12a_Program_Dist_Count)
dbExecute(decimal_con, qry_12b_Program_Dist_Total)
dbExecute(decimal_con, qry_12c_Program_Dist_Distribution)
dbExecute(decimal_con, "DROP TABLE qry_12a_Program_Dist_Count")
dbExecute(decimal_con, "DROP TABLE qry_12b_Program_Dist_Total")
dbExecute(decimal_con, "drop table qry_12_LCP4_LCIPPC_Recode_9999")
dbExecute(decimal_con, "drop table T_Predict_CIP_CRED_AGE_Flipped")

# check for combinations produced in static that were missed in the projected
dbGetQuery(decimal_con, qry_12d_Check_Missing)

# ---- Clean Up ----
# Lookups
dbExecute(decimal_con, "drop table AgeGroupLookup")
dbExecute(decimal_con, "drop table tbl_Age_Groups_Near_Completers")
dbExecute(decimal_con, "drop table tbl_Age_Groups")
dbExecute(decimal_con, "drop table T_Cohort_Program_Distributions_Y2_to_Y12")
dbExecute(decimal_con, "drop table T_APPR_Y2_to_Y10")
dbExecute(decimal_con, "drop table T_PSSM_Projection_Cred_Grp")
dbExecute(decimal_con, "drop table T_Weights_STP")

# Keep for next workflow
dbExistsTable(decimal_con, "Cohort_Program_Distributions_Projected")
dbExistsTable(decimal_con, "Cohort_Program_Distributions_Static")

# Keep in DB
dbExistsTable(decimal_con, "tbl_Program_Projection_Input")






