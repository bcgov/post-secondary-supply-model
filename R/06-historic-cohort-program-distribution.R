
library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# survey == 'PTIB' ---- 
## ptib

ptib <- tbl(decimal_con, "qry_Private_Credentials_06d1_Cohort_Dist") %>% collect() %>%
  rename(PSSM_CREDENTIAL = Credential) %>%
         mutate(YEAR = 2023) %>%
  select(-Year)
names(ptib) <- str_to_upper(names(ptib))

# survey == 'Program_Projections_2023-2024_qry_13d' ----
## near completers
dbExecute(decimal_con,"SELECT * 
              INTO [IDIR\\SYURCHAK].T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history
              FROM [IDIR\\LFREDRIC].T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history;")

near_completers <- dbGetQuery(decimal_con,
  "SELECT coci_subm_cd AS YEAR,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.PSSM_CREDENTIAL, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.PSSM_CRED, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.LCP4_CD, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.COSC_GRAD_STATUS_LGDS_CD_Group as GRAD_STATUS, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.TTRAIN AS COSC_TTRAIN, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.LCIP4_CRED, 
        CAST([COSC_GRAD_STATUS_LGDS_CD_Group] as NVARCHAR(50)) + ' - ' + Left([LCP4_CD],2) + ' - ' + CAST([TTRAIN] as NVARCHAR(50)) + ' - ' + [PSSM_CREDENTIAL] AS LCIP2_CRED, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.Age_Group as AGE_GROUP, 
        Sum(T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.[Near_completers_STP_Credentials]) AS [COUNT]
FROM    T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history
GROUP BY T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.PSSM_CREDENTIAL, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.PSSM_CRED, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.LCP4_CD, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.COSC_GRAD_STATUS_LGDS_CD_Group, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.TTRAIN, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.LCIP4_CRED, 
        CAST([COSC_GRAD_STATUS_LGDS_CD_Group] as NVARCHAR(50)) + ' - ' + Left([LCP4_CD],2) + ' - ' + CAST([TTRAIN] as NVARCHAR(50)) + ' - ' + [PSSM_CREDENTIAL], 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history.Age_Group,
        coci_subm_cd") %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(COUNT))  %>%
  ungroup() %>%
  inner_join(tbl(decimal_con, "tbl_Age_Groups_Near_Completers") %>% collect(),
            by = c("AGE_GROUP" = "AGE_GROUP_LABEL_NEAR_COMPLETER_PROJECTION"),
            relationship = "many-to-many") %>%
  mutate(SURVEY = 'Program_Projections_2023-2024_qry_13d',
         AGE_GROUP = AGE_GROUP_LABEL_GRADUATE_PROJECTION,
         YEAR = paste0("20", str_sub(YEAR, start = -2)) %>% as.numeric(),
         PERCENT = ifelse(TOTAL == 0, 0, COUNT/TOTAL)) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, 
         AGE_GROUP, YEAR, COUNT, TOTAL, PERCENT)
  
## survey = Program_Projections_2023-2024_Q012e ----
## ADCT or ADIP, ADGR or UT, BACH, CERT, DIPL, PDCT or PDDP
## Part 1 - STP
main_cohorts_stp <- dbGetQuery(decimal_con,
"SELECT PSI_AWARD_SCHOOL_YEAR_DELAYED AS YEAR,
        T_PSSM_Projection_Cred_Grp.PSSM_CREDENTIAL, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [PSSM_CREDENTIAL]) AS PSSM_CRED, 
        tbl_Program_Projection_Input.FINAL_CIP_CODE_4 AS LCP4_CD, 
        T_PSSM_Projection_Cred_Grp.COSC_GRAD_STATUS_LGDS_CD AS GRAD_STATUS, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [FINAL_CIP_CODE_4], ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_CREDENTIAL]) AS LCIP4_CRED, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, Left([FINAL_CIP_CODE_4],2), ' - ' , [T_PSSM_Projection_Cred_Grp].[PSSM_CREDENTIAL]) AS LCIP2_CRED, 
        tbl_Program_Projection_Input.AgeGroup AS AGE_GROUP, 
        Sum(tbl_Program_Projection_Input.Count) AS Counts, 
        T_Weights_STP.Weight, 
        Sum([Count])*([Weight]) AS Weighted
FROM    T_PSSM_Projection_Cred_Grp 
INNER JOIN (tbl_Program_Projection_Input 
  INNER JOIN T_Weights_STP 
    ON tbl_Program_Projection_Input.PSI_AWARD_SCHOOL_YEAR_DELAYED = T_Weights_STP.Year_Code) 
  ON T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential = tbl_Program_Projection_Input.PSI_CREDENTIAL_CATEGORY
WHERE   (((T_Weights_STP.Model)='2023-2024') AND ((T_PSSM_Projection_Cred_Grp.PSSM_CREDENTIAL) Not In ('APPRAPPR','APPRCERT','GRCT or GRDP','PDEG','MAST','DOCT')))
GROUP BY T_PSSM_Projection_Cred_Grp.PSSM_CREDENTIAL, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [PSSM_CREDENTIAL]), 
        tbl_Program_Projection_Input.FINAL_CIP_CODE_4, 
        T_PSSM_Projection_Cred_Grp.COSC_GRAD_STATUS_LGDS_CD, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [FINAL_CIP_CODE_4], ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_CREDENTIAL]), 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, Left([FINAL_CIP_CODE_4],2), ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_CREDENTIAL]), 
        tbl_Program_Projection_Input.AgeGroup, 
        T_Weights_STP.Weight,
        PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING (((T_Weights_STP.Weight)>0))") %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(Weighted))  %>%
  ungroup() %>%
  mutate(YEAR = str_sub(YEAR, start = 6) %>% as.numeric(),
         PERCENT = ifelse(TOTAL == 0, 0, Weighted/TOTAL))

## Part 2 TTRAIN
main_cohorts_TTRAIN <- dbGetQuery(decimal_con,
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
AND ((T_Cohorts_Recoded.Weight)>0));") %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, AGE_GROUP) %>%
  mutate(TOTAL = sum(Weighted)) %>%
  ungroup() %>%
  mutate(PERCENT = ifelse(TOTAL == 0, 0, Weighted/TOTAL))

## combine
main_cohorts <- main_cohorts_stp %>% 
  left_join(main_cohorts_TTRAIN %>% select(-PSSM_CRED), 
            b = c("YEAR", "PSSM_CREDENTIAL", "LCP4_CD", "GRAD_STATUS", "AGE_GROUP"),
            suffix = c("_STP", "_TTRAIN")) %>%
  mutate(SURVEY = "Program_Projections_2023-2024_Q012e",
         LCIP4_CRED = ifelse(is.na(LCIP4_CRED_TTRAIN), LCIP4_CRED_STP, LCIP4_CRED_TTRAIN),
         LCIP2_CRED = ifelse(is.na(LCIP2_CRED_TTRAIN), LCIP2_CRED_STP, LCIP2_CRED_TTRAIN),
         COUNT = ifelse(is.na(PERCENT_TTRAIN), Weighted_STP, Weighted_STP*PERCENT_TTRAIN),
         TOTAL = TOTAL_STP,
         PERCENT = COUNT/TOTAL) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, LCIP2_CRED, 
         AGE_GROUP, YEAR, COUNT, TOTAL, PERCENT)

# survey = 'Program_Projections_2023-2024_Q013e' ----
# pdeg: mast, doc
pdeg <- dbGetQuery(decimal_con,
  "SELECT PSI_AWARD_SCHOOL_YEAR_DELAYED AS YEAR,
        T_PSSM_Projection_Cred_Grp.PSSM_CREDENTIAL, 
		    CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [PSSM_CREDENTIAL]) AS PSSM_CRED, 
        qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD AS LCIPPC_CD, 
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ',
             [T_PSSM_Projection_Cred_Grp].[PSSM_CREDENTIAL]) AS LCIPPC_CRED, 
        tbl_Program_Projection_Input.AgeGroup as AGE_GROUP, 
        Sum(tbl_Program_Projection_Input.Count) AS Counts, 
        T_Weights_STP.Weight, 
        Sum([Count])*([Weight]) AS Weighted
FROM    (T_PSSM_Projection_Cred_Grp 
INNER JOIN (tbl_Program_Projection_Input   
    INNER JOIN T_Weights_STP 
      ON tbl_Program_Projection_Input.PSI_AWARD_SCHOOL_YEAR_DELAYED = T_Weights_STP.Year_Code) 
  ON T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential = tbl_Program_Projection_Input.PSI_CREDENTIAL_CATEGORY) 
INNER JOIN qry_12_LCP4_LCIPPC_Recode_9999 
  ON tbl_Program_Projection_Input.FINAL_CIP_CODE_4 = qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCP4_CD
WHERE (((T_Weights_STP.Model)='2023-2024') 
AND   ((T_PSSM_Projection_Cred_Grp.PSSM_CREDENTIAL) In ('GRCT or GRDP','PDEG','MAST','DOCT')))
GROUP BY T_PSSM_Projection_Cred_Grp.PSSM_CREDENTIAL, 
	    CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [PSSM_CREDENTIAL]), 
      qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD, 
      CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ',
      [T_PSSM_Projection_Cred_Grp].[PSSM_CREDENTIAL]), 
      tbl_Program_Projection_Input.AgeGroup, 
      T_Weights_STP.Weight,
      PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING (((T_Weights_STP.Weight)>0))") %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(Weighted)) %>% 
  ungroup() %>%
  mutate(SURVEY = 'Program_Projections_2023-2024_Q013e',
         YEAR = str_sub(YEAR, start = 6) %>% as.numeric(),
         PERCENT = ifelse(TOTAL == 0, 0, Weighted/TOTAL)) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD = LCIPPC_CD,
         LCIP4_CRED = LCIPPC_CRED, AGE_GROUP, YEAR, COUNT = Weighted, TOTAL, PERCENT)

# survey = 'Program_Projections_2023-2024_Q014e' ----
# apprenticeships
appso <- dbGetQuery(decimal_con, 
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
HAVING (((T_Cohorts_Recoded.Weight)>0))") %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(Weighted)) %>% 
  ungroup() %>%
  mutate(SURVEY = 'Program_Projections_2023-2024_Q014e',
         PERCENT = ifelse(TOTAL == 0, 0, Weighted/TOTAL)) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, 
         AGE_GROUP, YEAR, COUNT, TOTAL, PERCENT)

## combine ----
Cohort_Program_Distributions_history <- 
  bind_rows(ptib, 
            near_completers,
            main_cohorts,
            pdeg,
            appso)

dbWriteTable(decimal_con, 
             SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_history"')),
             Cohort_Program_Distributions_history)
