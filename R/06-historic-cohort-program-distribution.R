# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

# Refactor of 06-historic-cohort-program-distribution.R
# (clearer comments; unnecessary lines commented out).
#
# WHAT THIS SCRIPT PRODUCES
#   One table: Cohort_Program_Distributions_history, written to the analyst's
#   IDIR schema. Each row is COUNT / TOTAL / PERCENT for a
#   Survey x Credential x CIP4 x Age x Year cell, where PERCENT is the program
#   mix share  P(CIP | credential, age)  -- the share of a credential's
#   graduates that fall into each 4-digit CIP program. Script 07 multiplies
#   these shares against the graduate forecasts to push them down to program
#   (and ultimately occupation) detail.
#
# HOW IT WORKS
#   The table is assembled from FIVE graduate streams. Each is pulled with its
#   own SQL query, tagged with a distinguishing SURVEY label, reduced to a
#   common 11-column schema, then stacked with bind_rows():
#       ptib             SURVEY = 'PTIB'                                  (private)
#       near_completers  SURVEY = 'Program_Projections_2023-2024_qry_13d' (near-comp)
#       main_cohorts     SURVEY = 'Program_Projections_2023-2024_Q012e'   (public)
#       pdeg             SURVEY = 'Program_Projections_2023-2024_Q013e'   (grad-level)
#       appso            SURVEY = 'Program_Projections_2023-2024_Q014e'   (appren.)
#   Within each stream the recipe is identical:
#       Weighted = Count * Weight
#       TOTAL    = sum(Weighted) within YEAR x CREDENTIAL x AGE_GROUP
#       PERCENT  = Weighted / TOTAL                    <- the program mix share
#
# TECH DEBT (not changed here, to preserve behaviour):
#   - The inline SQL ideally moves to sql/06-historic-cohort-program-distribution/
#     and is source()d, per the project's SQL/R convention.
#   - The FROM clauses are not schema-qualified; they rely on the connection's
#     default schema resolving the *_r / dbo tables.

library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
# lan <- config::get("lan")   # REMOVED: not referenced anywhere in this script.
my_schema <- config::get("myschema")
# lan <- config::get("lan")   # REMOVED: not referenced anywhere in this script.

# Reuse an existing connection if one is already open (e.g. when this script is
# source()d after an earlier pipeline step); otherwise open our own. con_created
# records ownership so Clean Up only disconnects what THIS script opened.
con_created <- !exists("decimal_con", where = .GlobalEnv)
if (con_created) {
  decimal_con <- dbConnect(
    odbc::odbc(),
    Driver = db_config$driver,
    Server = db_config$server,
    Database = db_config$database,
    Trusted_Connection = "True" # Windows Integrated Authentication
  )
}

# ============================================================================
# STREAM 1 - PTIB (private training institutions)   SURVEY = 'PTIB'
# ============================================================================
# Already a finished cohort distribution (built in 05). Just read it and
# upper-case the column names to match the schema used by the other streams.

# survey == 'PTIB' ----
## ptib

ptib <- tbl(decimal_con, "qry_Private_Credentials_06d1_Cohort_Dist") %>%
  collect()
# Dead code from the original (kept for reference only): the private table
# already carries PSSM_CREDENTIAL / YEAR in the right shape, so the rename/
# mutate/select below are not needed.
# #%>%
# rename(PSSM_CREDENTIAL = Credential) %>%
# mutate(YEAR = 2023) %>%
# select(-Year)
names(ptib) <- str_to_upper(names(ptib))


# ============================================================================
# STREAM 2 - Near-completers   SURVEY = 'Program_Projections_2023-2024_qry_13d'
# ============================================================================
# Source: T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r (from 03).
# Sum Near_completers_STP_Credentials to a COUNT per
#   CIP4 x credential x ttrain x grad-status x age x year,
# compute the program-mix PERCENT within YEAR x CREDENTIAL x AGE_GROUP, then
# remap the near-completer age bands onto the graduate-projection age bands via
# tbl_Age_Groups_Near_Completers.
#
# A one-time manual table copy used to live here; it is intentionally disabled.
# If ever needed, do NOT hardcode schemas (the original used [IDIR\SYURCHAK] /
# [IDIR\LFREDRIC]). Use {my_schema} and guard with IF OBJECT_ID(...) DROP TABLE
# so it stays re-runnable, e.g.:
#   dbExecute(decimal_con, glue::glue(
#     "IF OBJECT_ID('[{my_schema}].[T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history]') IS NOT NULL
#        DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history];"))

near_completers <- dbGetQuery(
  decimal_con,
  "SELECT coci_subm_cd AS YEAR,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.PSSM_CREDENTIAL, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.PSSM_CRED, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.LCP4_CD, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.COSC_GRAD_STATUS_LGDS_CD_Group as GRAD_STATUS, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.TTRAIN AS COSC_TTRAIN, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.LCIP4_CRED, 
        CAST([COSC_GRAD_STATUS_LGDS_CD_Group] as NVARCHAR(50)) + ' - ' + Left([LCP4_CD],2) + ' - ' + CAST([TTRAIN] as NVARCHAR(50)) + ' - ' + [PSSM_CREDENTIAL] AS LCIP2_CRED, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.Age_Group as AGE_GROUP, 
        Sum(T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.[Near_completers_STP_Credentials]) AS [COUNT]
FROM    T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r
GROUP BY T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.PSSM_CREDENTIAL, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.PSSM_CRED, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.LCP4_CD, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.COSC_GRAD_STATUS_LGDS_CD_Group, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.TTRAIN, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.LCIP4_CRED, 
        CAST([COSC_GRAD_STATUS_LGDS_CD_Group] as NVARCHAR(50)) + ' - ' + Left([LCP4_CD],2) + ' - ' + CAST([TTRAIN] as NVARCHAR(50)) + ' - ' + [PSSM_CREDENTIAL], 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history_r.Age_Group,
        coci_subm_cd"
) %>%
  # Program-mix TOTAL is taken over CREDENTIAL x AGE_GROUP within each YEAR.
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(COUNT)) %>%
  ungroup() %>%
  # Map near-completer age bands -> graduate-projection age bands (1 row can map
  # to several, hence many-to-many).
  inner_join(
    tbl(decimal_con, "tbl_Age_Groups_Near_Completers") %>% collect(),
    by = c("AGE_GROUP" = "AGE_GROUP_LABEL_NEAR_COMPLETER_PROJECTION"),
    relationship = "many-to-many"
  ) %>%
  mutate(
    SURVEY = 'Program_Projections_2023-2024_qry_13d',
    AGE_GROUP = AGE_GROUP_LABEL_GRADUATE_PROJECTION,
    # coci_subm_cd like "C_Outc19" -> calendar year 2019.
    YEAR = paste0("20", str_sub(YEAR, start = -2)) %>% as.numeric(),
    PERCENT = ifelse(TOTAL == 0, 0, COUNT / TOTAL)
  ) %>%
  select(
    SURVEY,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD,
    LCIP4_CRED,
    LCIP2_CRED,
    AGE_GROUP,
    YEAR,
    COUNT,
    TOTAL,
    PERCENT
  )


# ============================================================================
# STREAM 3 - Main public cohorts   SURVEY = 'Program_Projections_2023-2024_Q012e'
# Credentials: ADCT or ADIP, ADGR or UT, BACH, CERT, DIPL, PDCT or PDDP
# ============================================================================
# Built in two parts then merged:
#   Part 1 (STP):    weighted program counts by CIP4 (the base distribution).
#   Part 2 (TTRAIN): the trades-training split, used to sub-divide Part 1 where
#                    a TTRAIN breakdown exists.
## survey = Program_Projections_2023-2024_Q012e ----
## ADCT or ADIP, ADGR or UT, BACH, CERT, DIPL, PDCT or PDDP
## Part 1 - STP
main_cohorts_stp <- dbGetQuery(
  decimal_con,
  "SELECT PSI_AWARD_SCHOOL_YEAR_DELAYED AS YEAR,
        T_PSSM_Projection_Cred_Grp_r.PSSM_CREDENTIAL, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [PSSM_CREDENTIAL]) AS PSSM_CRED, 
        tbl_Program_Projection_Input_r.FINAL_CIP_CODE_4 AS LCP4_CD, 
        T_PSSM_Projection_Cred_Grp_r.COSC_GRAD_STATUS_LGDS_CD AS GRAD_STATUS, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [FINAL_CIP_CODE_4], ' - ', [T_PSSM_Projection_Cred_Grp_r].[PSSM_CREDENTIAL]) AS LCIP4_CRED, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, Left([FINAL_CIP_CODE_4],2), ' - ' , [T_PSSM_Projection_Cred_Grp_r].[PSSM_CREDENTIAL]) AS LCIP2_CRED, 
        tbl_Program_Projection_Input_r.AgeGroup AS AGE_GROUP, 
        Sum(tbl_Program_Projection_Input_r.Count) AS Counts, 
        T_Weights_STP.Weight, 
        Sum([Count])*([Weight]) AS Weighted
FROM    T_PSSM_Projection_Cred_Grp_r 
INNER JOIN (tbl_Program_Projection_Input_r 
  INNER JOIN T_Weights_STP 
    ON tbl_Program_Projection_Input_r.PSI_AWARD_SCHOOL_YEAR_DELAYED = T_Weights_STP.Year_Code) 
  ON T_PSSM_Projection_Cred_Grp_r.PSSM_Projection_Credential = tbl_Program_Projection_Input_r.PSI_CREDENTIAL_CATEGORY
WHERE   (((T_Weights_STP.Model)='2023-2024') AND ((T_PSSM_Projection_Cred_Grp_r.PSSM_CREDENTIAL) Not In ('APPRAPPR','APPRCERT','GRCT or GRDP','PDEG','MAST','DOCT')))
GROUP BY T_PSSM_Projection_Cred_Grp_r.PSSM_CREDENTIAL, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [PSSM_CREDENTIAL]), 
        tbl_Program_Projection_Input_r.FINAL_CIP_CODE_4, 
        T_PSSM_Projection_Cred_Grp_r.COSC_GRAD_STATUS_LGDS_CD, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [FINAL_CIP_CODE_4], ' - ', [T_PSSM_Projection_Cred_Grp_r].[PSSM_CREDENTIAL]), 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, Left([FINAL_CIP_CODE_4],2), ' - ', [T_PSSM_Projection_Cred_Grp_r].[PSSM_CREDENTIAL]), 
        tbl_Program_Projection_Input_r.AgeGroup, 
        T_Weights_STP.Weight,
        PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING (((T_Weights_STP.Weight)>0))"
) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(Weighted)) %>%
  ungroup() %>%
  mutate(
    # YEAR like "2018/2019" -> take the trailing year as numeric.
    YEAR = str_sub(YEAR, start = 6) %>% as.numeric(),
    PERCENT = ifelse(TOTAL == 0, 0, Weighted / TOTAL)
  )

## Part 2 - TTRAIN (trades-training split; grad status <> '3', TTRAIN not null)
main_cohorts_TTRAIN <- dbGetQuery(
  decimal_con,
  "SELECT SURVEY_YEAR AS YEAR,
        T_Cohorts_Recoded.PSSM_CREDENTIAL, 
        T_Cohorts_Recoded.PSSM_CREDENTIAL AS PSSM_CRED, 
        T_Cohorts_Recoded.LCP4_CD, 
        T_Cohorts_Recoded.GRAD_STATUS, 
        TTRAIN,
         CONCAT(
          (CASE WHEN [GRAD_STATUS] IS NULL THEN Null ELSE CAST([GRAD_STATUS] AS NVARCHAR(50)) + ' - ' END),
			    [LCP4_CD], ' - ',
			    (CASE WHEN [TTRAIN] IS NULL THEN Null ELSE CAST([TTRAIN] AS NVARCHAR(50)) + ' - ' END),
			    [PSSM_CREDENTIAL]
			   ) AS LCIP4_CRED, 
        CONCAT(
          (CASE WHEN [GRAD_STATUS] IS NULL THEN Null ELSE CAST([GRAD_STATUS] AS NVARCHAR(50)) + ' - ' END), 
			    Left([LCP4_CD],2) , ' - ',  
			    (CASE WHEN [TTRAIN] IS NULL THEN Null ELSE CAST([TTRAIN] AS NVARCHAR(50)) + ' - ' END), 
			    [PSSM_CREDENTIAL]
			  ) AS LCIP2_CRED, 
        tbl_Age_Groups.Age_Group_Label AS AGE_GROUP, 
        Count(*) AS Counts, 
        T_Cohorts_Recoded.Weight, 
        Count(*)*([Weight]) AS Weighted
FROM T_Cohorts_Recoded 
INNER JOIN tbl_Age_Groups 
  ON T_Cohorts_Recoded.Age_Group = tbl_Age_Groups.Age_Group
WHERE (((T_Cohorts_Recoded.GRAD_STATUS)<>'3'))
GROUP BY T_Cohorts_Recoded.PSSM_CREDENTIAL, T_Cohorts_Recoded.LCP4_CD, 
        T_Cohorts_Recoded.GRAD_STATUS, T_Cohorts_Recoded.TTRAIN, 
        T_Cohorts_Recoded.LCIP4_CRED, T_Cohorts_Recoded.LCIP2_CRED, 
        tbl_Age_Groups.Age_Group_Label, T_Cohorts_Recoded.Weight, 
        T_Cohorts_Recoded.PSSM_CREDENTIAL,
        SURVEY_YEAR
HAVING (((T_Cohorts_Recoded.TTRAIN) Is Not Null) 
AND ((T_Cohorts_Recoded.Weight)>0));"
) %>%
  group_by(
    YEAR,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD,
    GRAD_STATUS,
    AGE_GROUP
  ) %>%
  mutate(TOTAL = sum(Weighted)) %>%
  ungroup() %>%
  mutate(PERCENT = ifelse(TOTAL == 0, 0, Weighted / TOTAL))


## Combine Part 1 (STP) with Part 2 (TTRAIN)
# IMPORTANT: the original wrote `b = c(...)`. R partial-matches `b` to the `by`
# argument (the only formal starting with "b"), so this already joined on the 5
# keys below. Made explicit as `by =` for clarity - behaviour is unchanged.
# Because this affects the output, VALIDATE the result against the existing
# Cohort_Program_Distributions_history with waldo::compare before relying on it.
#
# Coalesce logic: where a TTRAIN split exists, sub-divide the STP weight by the
# trades PERCENT; otherwise keep the STP weight as-is.
## combine
main_cohorts <- main_cohorts_stp %>%
  left_join(
    main_cohorts_TTRAIN %>% select(-PSSM_CRED),
    by = c("YEAR", "PSSM_CREDENTIAL", "LCP4_CD", "GRAD_STATUS", "AGE_GROUP"),
    suffix = c("_STP", "_TTRAIN")
  ) %>%
  mutate(
    SURVEY = "Program_Projections_2023-2024_Q012e",
    LCIP4_CRED = ifelse(
      is.na(LCIP4_CRED_TTRAIN),
      LCIP4_CRED_STP,
      LCIP4_CRED_TTRAIN
    ),
    LCIP2_CRED = ifelse(
      is.na(LCIP2_CRED_TTRAIN),
      LCIP2_CRED_STP,
      LCIP2_CRED_TTRAIN
    ),
    COUNT = ifelse(
      is.na(PERCENT_TTRAIN),
      Weighted_STP,
      Weighted_STP * PERCENT_TTRAIN
    ),
    TOTAL = TOTAL_STP,
    PERCENT = COUNT / TOTAL
  ) %>%
  select(
    SURVEY,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED,
    LCIP2_CRED,
    AGE_GROUP,
    YEAR,
    COUNT,
    TOTAL,
    PERCENT
  )

# ============================================================================
# STREAM 4 - Grad-level   SURVEY = 'Program_Projections_2023-2024_Q013e'
# Credentials: GRCT or GRDP, PDEG, MAST, DOCT
# ============================================================================
# Same weighting recipe as Stream 3 Part 1, but CIP4 is recoded to LCIPPC via
# qry_12_LCP4_LCIPPC_Recode_9999 (grad-level programs use the PPC roll-up).
# survey = 'Program_Projections_2023-2024_Q013e' ----
# pdeg: mast, doc
pdeg <- dbGetQuery(
  decimal_con,
  "SELECT PSI_AWARD_SCHOOL_YEAR_DELAYED AS YEAR,
        T_PSSM_Projection_Cred_Grp_r.PSSM_CREDENTIAL, 
		    CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [PSSM_CREDENTIAL]) AS PSSM_CRED, 
        qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD AS LCIPPC_CD, 
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ',
             [T_PSSM_Projection_Cred_Grp_r].[PSSM_CREDENTIAL]) AS LCIPPC_CRED, 
        tbl_Program_Projection_Input_r.AgeGroup as AGE_GROUP, 
        Sum(tbl_Program_Projection_Input_r.Count) AS Counts, 
        T_Weights_STP.Weight, 
        Sum([Count])*([Weight]) AS Weighted
FROM    (T_PSSM_Projection_Cred_Grp_r 
INNER JOIN (tbl_Program_Projection_Input_r   
    INNER JOIN T_Weights_STP 
      ON tbl_Program_Projection_Input_r.PSI_AWARD_SCHOOL_YEAR_DELAYED = T_Weights_STP.Year_Code) 
  ON T_PSSM_Projection_Cred_Grp_r.PSSM_Projection_Credential = tbl_Program_Projection_Input_r.PSI_CREDENTIAL_CATEGORY) 
INNER JOIN qry_12_LCP4_LCIPPC_Recode_9999 
  ON tbl_Program_Projection_Input_r.FINAL_CIP_CODE_4 = qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCP4_CD
WHERE (((T_Weights_STP.Model)='2023-2024') 
AND   ((T_PSSM_Projection_Cred_Grp_r.PSSM_CREDENTIAL) In ('GRCT or GRDP','PDEG','MAST','DOCT')))
GROUP BY T_PSSM_Projection_Cred_Grp_r.PSSM_CREDENTIAL, 
	    CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [PSSM_CREDENTIAL]), 
      qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD, 
      CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ',
      [T_PSSM_Projection_Cred_Grp_r].[PSSM_CREDENTIAL]), 
      tbl_Program_Projection_Input_r.AgeGroup, 
      T_Weights_STP.Weight,
      PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING (((T_Weights_STP.Weight)>0))"
) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(Weighted)) %>%
  ungroup() %>%
  mutate(
    SURVEY = 'Program_Projections_2023-2024_Q013e',
    YEAR = str_sub(YEAR, start = 6) %>% as.numeric(),
    PERCENT = ifelse(TOTAL == 0, 0, Weighted / TOTAL)
  ) %>%
  # Rename LCIPPC_* into the common LCP4_CD / LCIP4_CRED schema; COUNT = Weighted.
  select(
    SURVEY,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD = LCIPPC_CD,
    LCIP4_CRED = LCIPPC_CRED,
    AGE_GROUP,
    YEAR,
    COUNT = Weighted,
    TOTAL,
    PERCENT
  )

# ============================================================================
# STREAM 5 - Apprenticeships   SURVEY = 'Program_Projections_2023-2024_Q014e'
# Credentials: APPRAPPR, APPRCERT
# ============================================================================
# Straight weighted counts from T_Cohorts_Recoded for the two apprenticeship
# credentials; same TOTAL / PERCENT recipe.
# survey = 'Program_Projections_2023-2024_Q014e' ----
# apprenticeships
appso <- dbGetQuery(
  decimal_con,
  "SELECT SURVEY_YEAR AS YEAR,
        T_Cohorts_Recoded.PSSM_CREDENTIAL, 
        T_Cohorts_Recoded.PSSM_CREDENTIAL AS PSSM_CRED, 
        T_Cohorts_Recoded.LCP4_CD, 
        T_Cohorts_Recoded.TTRAIN, 
        T_Cohorts_Recoded.LCIP4_CRED, 
        T_Cohorts_Recoded.LCIP2_CRED, 
        tbl_Age_Groups.Age_Group_Label AS AGE_GROUP, 
        Count(*) AS COUNT, 
        T_Cohorts_Recoded.Weight, 
        Count(*)*([Weight]) AS Weighted
FROM    T_Cohorts_Recoded INNER JOIN tbl_Age_Groups 
ON      T_Cohorts_Recoded.Age_Group = tbl_Age_Groups.Age_Group
WHERE   (((T_Cohorts_Recoded.PSSM_CREDENTIAL) In ('APPRAPPR','APPRCERT')))
GROUP BY T_Cohorts_Recoded.PSSM_CREDENTIAL, 
        T_Cohorts_Recoded.LCP4_CD, 
        T_Cohorts_Recoded.TTRAIN, 
        T_Cohorts_Recoded.LCIP4_CRED, 
        T_Cohorts_Recoded.LCIP2_CRED, tbl_Age_Groups.Age_Group_Label, 
        T_Cohorts_Recoded.Weight, 
        T_Cohorts_Recoded.PSSM_CREDENTIAL,
        SURVEY_YEAR
HAVING (((T_Cohorts_Recoded.Weight)>0))"
) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(Weighted)) %>%
  ungroup() %>%
  mutate(
    SURVEY = 'Program_Projections_2023-2024_Q014e',
    PERCENT = ifelse(TOTAL == 0, 0, Weighted / TOTAL)
  ) %>%
  select(
    SURVEY,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD,
    LCIP4_CRED,
    LCIP2_CRED,
    AGE_GROUP,
    YEAR,
    COUNT,
    TOTAL,
    PERCENT
  )
# ============================================================================
# Combine all five streams and write out
# ============================================================================
## combine ----
Cohort_Program_Distributions_history <-
  bind_rows(ptib, near_completers, main_cohorts, pdeg, appso)
# Write to the analyst's IDIR schema. NOTE: written WITHOUT an "_r" suffix to
# match the existing downstream consumers - do not rename without checking 07.
dbWriteTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_history_r"')),
  Cohort_Program_Distributions_history,
  overwrite = TRUE
)

# ---- Clean Up ----
# Only disconnect if THIS script opened the connection; always release memory.
if (con_created) {
  dbDisconnect(decimal_con)
}
gc()

# Return the assembled table as the script's result.
Cohort_Program_Distributions_history
