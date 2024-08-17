library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

source(glue::glue("{lan}/development/sql/gh-source/06-program-projections/06-program-projections.R"))

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
# but this is how it was done in prior years
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

# ---- Near Completers ----
# build and insert Program_Projections_2019-2020_qry_13d into Cohort_Program_Distributions_Projected (930)
# build and insert Program_Projections_2019-2020_qry_13d into Cohort_Program_Distributions_Static (930)
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


# ---- Static Program Distributions ----
# survey == 'Program_Projections_2019-2020_Q012e'
# TO DO: check counts
dbGetQuery(decimal_con, Q012a_Check_Total_for_Invalid_CIPs)
dbExecute(decimal_con, Q012b_Weight_Cohort_Dist)
dbExecute(decimal_con, Q012c_Weighted_Cohort_Dist)
dbExecute(decimal_con, Q012c1_Weighted_Cohort_Dist_TTRAIN)
dbExecute(decimal_con, Q012c2_Weighted_Cohort_Dist)
dbExecute(decimal_con, Q012c3_Weighted_Cohort_Dist_Total)
dbExecute(decimal_con, Q012c4_Weighted_Cohort_Distribution_Projected)
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

# survey == 'Program_Projections_2019-2020_Q013e'
dbGetQuery(decimal_con, Q013a_Check_PDEG_CLP_07_Only_CIP_22)
dbExecute(decimal_con, Q013b_Weight_Cohort_Dist_MAST_DOCT_Others)
dbExecute(decimal_con, Q013c_Weighted_Cohort_Dist)
dbExecute(decimal_con, Q013d_Weighted_Cohort_Dist_Total)
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Static 
          WHERE Survey LIKE 'Program_Projections_2019-2020_Q013e'") # Added
dbExecute(decimal_con, Q013e_Weighted_Cohort_Distribution)
dbExecute(decimal_con, "drop table Q013b_Weight_Cohort_Dist_MAST_DOCT_Others")
dbExecute(decimal_con, "drop table Q013c_Weighted_Cohort_Dist")
dbExecute(decimal_con, "drop table Q013d_Weighted_Cohort_Dist_Total")

# survey == 'Program_Projections_2019-2020_Q014e' (Static and Projected) 
dbExecute(decimal_con, Q014b_Weighted_Cohort_Dist_APPR)
dbExecute(decimal_con, Q014c_Weighted_Cohort_Dist)
dbExecute(decimal_con, Q014d_Weighted_Cohort_Dist_Total)
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Projected 
          WHERE Survey LIKE 'Program_Projections_2019-2020_Q014e'") # Added
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Static 
          WHERE Survey LIKE 'Program_Projections_2019-2020_Q014e'") # Added
dbExecute(decimal_con, Q014e_Weighted_Cohort_Distribution_Projected)
dbExecute(decimal_con, Q014e_Weighted_Cohort_Distribution_Static )
dbExecute(decimal_con, "drop table Q014b_Weighted_Cohort_Dist_APPR")
dbExecute(decimal_con, "drop table Q014c_Weighted_Cohort_Dist")
dbExecute(decimal_con, "drop table Q014d_Weighted_Cohort_Dist_Total")

# Extra Apprenticeship work, I'm not sure yet how this fits in.
Q000_TRD_Graduates <- dbGetQuery(decimal_con, "SELECT * FROM Q000_TRD_Graduates")
APPSO_Graduates <- dbGetQuery(decimal_con, "SELECT * FROM APPSO_Graduates")

appso_2_yr_avg <- APPSO_Graduates %>% 
  summarize(n = sum(EXPR1, na.rm = TRUE), .by = c(SUBM_CD, PSSM_CREDENTIAL, AGE_GROUP_LABEL)) %>%
  filter(SUBM_CD %in% c('C_Outc17','C_Outc18')) %>%
  summarize(avg = sum(n/2, na.rm = TRUE), .by = c(PSSM_CREDENTIAL, AGE_GROUP_LABEL))

trd_2_yr_avg <- Q000_TRD_Graduates  %>% 
  summarize(n = sum(EXPR1), .by = c(SUBM_CD, PSSM_CREDENTIAL, AGE_GROUP_LABEL)) %>%
  filter(SUBM_CD %in% c('C_Outc17','C_Outc18')) %>%
  summarize(avg = sum(n/2, na.rm = TRUE), .by = c(PSSM_CREDENTIAL, AGE_GROUP_LABEL))

# TO DO: update T_APR_Y2_to_Y10 like the T_Cohort_Program_Distributions_Y2_to_Y12 table
# TO DO: Q014f_APPSO_Grads_Y2_to_Y10 to append other years static graduate projections


# survey == 'Program_Projections_2019-2020_Q015e21' (Static and Projected) 
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Projected 
          WHERE Survey LIKE 'Program_Projections_2019-2020_Q015e21'") # Added
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Static 
          WHERE Survey LIKE 'Program_Projections_2019-2020_Q015e22'") # Added
dbExecute(decimal_con, Q015e21_Append_Selected_Static_Distribution_Y2_to_Y12_Projected)
dbExecute(decimal_con, Q015e22_Append_Distribution_Y2_to_Y12_Static)

# ----  Run Werner Program ----
input_data <- dbGetQuery(decimal_con, "SELECT * FROM tbl_Program_Projection_Input") %>% 
  select(-Expr1) %>%
  complete(AgeGroup, PSI_CREDENTIAL_CATEGORY, FINAL_CIP_CODE_4, PSI_AWARD_SCHOOL_YEAR_DELAYED, fill = list(Count = 0)) %>% 
  pivot_wider(names_from = "PSI_AWARD_SCHOOL_YEAR_DELAYED", values_from = "Count") %>%
  rename("CIP" = "FINAL_CIP_CODE_4", 
         "AGE" = "AgeGroup", 
         "CRED" = "PSI_CREDENTIAL_CATEGORY")
write_csv(input_data, glue::glue("{lan}/development/csv/gh-source/tmp/input-data.csv"))

# run werner program
# TO DO: run program and join on age, cip etc
output_data <- read_delim(glue::glue("{lan}/development/csv/gh-source/tmp/output.csv"), delim = "\t", col_names = TRUE)
names(output_data) <- paste0(2019:(2019+11), "/", 2020:(2020+11))
as.data.frame(output_data)

T_Predict_CIP_CRED_AGE <- input_data %>% inner_join(output_data, .by = c(AGE, CRED, CIP))
dbWriteTable(decimal_con, "T_Predict_CIP_CRED_AGE", T_Predict_CIP_CRED_AGE)

# pivot T_Predict_CIP_CRED_AGE from wide to long
T_Predict_CIP_CRED_AGE_Flipped <- T_Predict_CIP_CRED_AGE %>% 
  pivot_longer(-c(CIP, CRED, AGE), names_to = "Year", values_to = "Count") %>%
  filter(Year %in% c('2019/2020', '2020/2021','2021/2022','2022/2023','2023/2024','2024/2025','2025/2026',
                   '2026/2027','2027/2028','2028/2029','2029/2030','2030/2031'))
dbWriteTable(decimal_con, "T_Predict_CIP_CRED_AGE_Flipped", T_Predict_CIP_CRED_AGE_Flipped)
dbGetQuery(decimal_con, qry_05_Flip_T_Predict_CIP_CRED_AGE_2_Check)

dbExecute(decimal_con, qry_09_Delete_Selected_Static_Cohort_Dist_from_Projected)

# survey == 'Program_Projections_2019-2020_qry10c'
dbExecute(decimal_con, qry_10a_Program_Dist_Count)
dbExecute(decimal_con, qry_10b_Program_Dist_Total)
dbExecute(decimal_con, qry_10c_Program_Dist_Distribution)
dbExecute(decimal_con, "DROP TABLE qry_10a_Program_Dist_Count")
dbExecute(decimal_con, "DROP TABLE qry_10b_Program_Dist_Total")

# survey == 'Program_Projections_2019-2020_qry12c'
dbExecute(decimal_con, qry_12a_Program_Dist_Count)
dbExecute(decimal_con, qry_12b_Program_Dist_Total)
dbExecute(decimal_con, qry_12c_Program_Dist_Distribution)
dbExecute(decimal_con, "DROP TABLE qry_12a_Program_Dist_Count")
dbExecute(decimal_con, "DROP TABLE qry_12b_Program_Dist_Total")

# TO DO: check numbers
dbGetQuery(decimal_con, qry_12d_Check_Missing)
# TO DO: when is this supposed to be run? why?
dbExecute(decimal_con, qry_12_LCP4_LCIPPC_Recode_9999)
dbExecute(decimal_con, "drop table qry_12_LCP4_LCIPPC_Recode_9999")

# ---- Clean Up -----
dbExecute(decimal_con, "drop table T_Predict_CIP_CRED_AGE_Flipped")

# ---- Clean Up ----
dbExecute(decimal_con, "drop table Cohort_Program_Distributions_Projected")
dbExecute(decimal_con, "drop table Cohort_Program_Distributions_Static")
dbExecute(decimal_con, "drop table T_Cohort_Program_Distributions_Y2_to_Y12")






