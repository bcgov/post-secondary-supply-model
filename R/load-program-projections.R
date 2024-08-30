# ******************************************************************************
# Load datasets required to run program projections step
# ******************************************************************************

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("pdbtrn")
jdbc_driver_config <- config::get("jdbc")
lan <- config::get("lan")

# ---- Connection to outcomes ----
jdbcDriver <- JDBC(driverClass = jdbc_driver_config$class,
                   classPath = jdbc_driver_config$path)

outcomes_con <- dbConnect(drv = jdbcDriver, 
                          url = db_config$url,
                          user = db_config$user,
                          password = db_config$password)

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")


# ---- Lookups  ----
T_Cohort_Program_Distributions_Y2_to_Y12 <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/T_Cohort_Program_Distributions_Y2_to_Y12.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

T_APPR_Y2_to_Y10 <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/T_APPR_Y2_to_Y10.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

tbl_Age_Groups_Near_Completers <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/tbl_Age_Groups_Near_Completers.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

tbl_Age_Groups <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

T_PSSM_Projection_Cred_Grp  <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/T_PSSM_Projection_Cred_Grp.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

T_Weights_STP <- 
readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/T_Weights_STP.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

AgeGroupLookup <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/AgeGroupLookup.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# From outcomes
INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbGetQuery(outcomes_con, "SELECT * FROM L_CIP_4DIGITS_CIP2016")
INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbGetQuery(outcomes_con, "SELECT * FROM L_CIP_6DIGITS_CIP2016")

# check that only required survey years are in T_Cohorts_Recoded
stopifnot(exprs = {
  dbGetQuery(decimal_con, "select distinct survey_year from T_Cohorts_Recoded")$survey_year==c(2019:2023)
})

# ---- Rollover data ----
Cohort_Program_Distributions_Projected <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/rollover/06/Cohort_Program_Distributions_Projected.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

Cohort_Program_Distributions_Static <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/rollover/06/Cohort_Program_Distributions_Static.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")


# ---- Build tbl_Program_Projection_Input ---- 
qry_Build_Program_Projection_Input <- "
--CREATE VIEW tbl_Program_Projection_Input AS 
SELECT  A.AgeGroup, 
        tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
        tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + A.AgeGroup AS Expr1,
        Credential_Non_Dup.FINAL_CIP_CODE_4, 
        tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, 
COUNT(*) AS Count 
FROM    tblCredential_HighestRank 
INNER JOIN pssm2019.dbo.AgeGroupLookup A
  ON    tblCredential_HighestRank.AGE_GROUP_AT_GRAD = A.AgeIndex 
INNER JOIN Credential_Non_Dup 
  ON    tblCredential_HighestRank.id = Credential_Non_Dup.id 
WHERE   (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') 
  AND   (tblCredential_HighestRank.RESEARCH_UNIVERSITY = 1) 
  AND   (tblCredential_HighestRank.OUTCOMES_CRED <> 'DACSO') 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') 
  OR    (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) 
  AND   (tblCredential_HighestRank.RESEARCH_UNIVERSITY = 1) 
  AND   (tblCredential_HighestRank.OUTCOMES_CRED <> 'DACSO') 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') 
  OR    (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') 
  AND   (tblCredential_HighestRank.RESEARCH_UNIVERSITY IS NULL) 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') 
  OR    (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) 
  AND   (tblCredential_HighestRank.RESEARCH_UNIVERSITY IS NULL) 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') 
GROUP BY A.AgeGroup, 
        tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
        tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED,  
        Credential_Non_Dup.FINAL_CIP_CODE_4 
HAVING  (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')"

tbl_Program_Projection_Input <- dbGetQuery(decimal_con, qry_Build_Program_Projection_Input)

# ---- Write to decimal ----
dbWriteTable(decimal_con, name = "tbl_Age_Groups_Near_Completers", tbl_Age_Groups_Near_Completers)
dbWriteTable(decimal_con, name = "tbl_Age_Groups", tbl_Age_Groups, overwrite=TRUE)
dbWriteTable(decimal_con, name = "T_Cohort_Program_Distributions_Y2_to_Y12",  T_Cohort_Program_Distributions_Y2_to_Y12)
dbWriteTable(decimal_con, name = "T_APPR_Y2_to_Y10",  T_APPR_Y2_to_Y10)
dbWriteTable(decimal_con, name = "INFOWARE_L_CIP_4DIGITS_CIP2016", INFOWARE_L_CIP_4DIGITS_CIP2016)
dbWriteTable(decimal_con, name = "INFOWARE_L_CIP_6DIGITS_CIP2016", INFOWARE_L_CIP_6DIGITS_CIP2016)
dbWriteTable(decimal_con, name = "T_PSSM_Projection_Cred_Grp", T_PSSM_Projection_Cred_Grp)
dbWriteTable(decimal_con, name = "T_Weights_STP",  T_Weights_STP)

dbWriteTable(decimal_con, name = "tbl_Program_Projection_Input", tbl_Program_Projection_Input)

# ---- Rollover ----
dbWriteTable(decimal_con, name = "Cohort_Program_Distributions_Static",  Cohort_Program_Distributions_Static)
dbWriteTable(decimal_con, name = "Cohort_Program_Distributions_Projected",  Cohort_Program_Distributions_Projected)


# ---- Disconnect ----
dbDisconnect(decimal_con)
dbDisconnect(outcomes_con)
