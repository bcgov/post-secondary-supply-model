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

# This script computes the ratio of near completers to graduates by age group and credential
# Near completers who later received a credential according to the STP Credential
# file or had an earlier credential are subtracted from the total of all near completers.
#
# Age groups: 17 to 19, 20 to 24, 25 to 29, and 35 to 64
# Credentials: From Diploma, Associate Degree, and Certificate Outcomes Survey cohorts.
# Survey years: 2018, 2019, 2020, 2021, 2022, 2023 for PSSM 2023
# STP Credential years searched: 2002/03 - 2022/23
#
# Annual ratios are computed for all available years and an average taken of two or three representative years
# (chosen by investigation).  PSSM model 2023 used an average ratio of 2018-2019.
# Notes: Using age at grad (not age at survey) for age groupings.

library(tidyverse)
library(DBI)
library(odbc)
library(config)

# ---- Configure LAN and file paths ----
#db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")
#db_schema <- config::get("dbschema")

# ---- Connection to database ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# ---- Data Requirements and SQL Definitons ----
#source("./sql/03-near-completers/near-completers-investigation-ttrain.R")
#source("./sql/03-near-completers/dacso-near-completers.R")

# tables made in earlier part of workflow
# for testing, copy T_DACSO_Data_Part_1 from dbo.  Drop Age_At_Grad
t_dacso_data_part_1 <- dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."t_dacso_data_part_1"'))
) |>
  select(-Age_At_Grad)

credential_non_dup <- dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"'))
)

stp_credential <- dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."STP_Credential"'))
)

# "rollover table" - this data is provisioned from SO
years <- 2018:2023
# write to Decimal as tmp_tbl_Age_AppendNewYears
tmp_tbl_age_append_new_years <- years |>
  purrr::map_dfr(
    ~ {
      file_path <- glue::glue(
        "{lan}/data/student-outcomes/csv/so-provision/qry_make_tmp_table_Age_step1_{.x}.csv"
      )
      read_csv(file_path, col_types = "dcdcd")
    }
  )

# write to Decimal as tmp_tbl_Age
tmp_tbl_age <- read_csv(
  glue::glue(
    "{lan}/development/csv/gh-source/testing/03/tmp_tbl_Age.csv"
  ),
  col_types = "dccccdd"
) |>
  mutate(
    TPID_DATE_OF_BIRTH = as.Date(TPID_DATE_OF_BIRTH),
    COSC_ENRL_END_DATE = as.Date(COSC_ENRL_END_DATE),
    COSC_GRAD_CREDENTIAL_DATE = as.Date(COSC_GRAD_CREDENTIAL_DATE)
  )

# lookups
tbl_age <- tibble(
  Age = 0:150
) %>%
  mutate(
    Age_Group = case_when(
      Age >= 15 & Age <= 16 ~ 1,
      Age >= 17 & Age <= 19 ~ 2,
      Age >= 20 & Age <= 24 ~ 3,
      Age >= 25 & Age <= 29 ~ 4,
      Age >= 30 & Age <= 34 ~ 5,
      Age >= 35 & Age <= 44 ~ 6,
      Age >= 45 & Age <= 54 ~ 7,
      Age >= 55 & Age <= 64 ~ 8,
      Age >= 65 & Age < 90 ~ 9,
      Age >= 90 ~ NA_real_,
      TRUE ~ NA_real_ # For Age < 15
    )
  )

t_pssm_projection_cred_grp <- tibble(
  PSSM_Projection_Credential = c(
    "ADVANCED CERTIFICATE",
    "ASSOCIATE DEGREE",
    "ADVANCED DIPLOMA",
    "BACHELORS DEGREE",
    "CERTIFICATE",
    "DIPLOMA",
    "DOCTORATE",
    "GRADUATE CERTIFICATE",
    "MASTERS DEGREE",
    "POST-DEGREE CERTIFICATE",
    "POST-DEGREE DIPLOMA",
    "FIRST PROFESSIONAL DEGREE",
    "GRADUATE DIPLOMA",
    "APPRAPPR",
    "APPRCERT"
  ),
  PSSM_Credential = c(
    "ADCT or ADIP",
    "ADGR or UT",
    "ADCT or ADIP",
    "BACH",
    "CERT",
    "DIPL",
    "DOCT",
    "GRCT or GRDP",
    "MAST",
    "PDCT or PDDP",
    "PDCT or PDDP",
    "PDEG",
    "GRCT or GRDP",
    "APPRAPPR",
    "APPRCERT"
  ),
  PSSM_Credential_Name = c(
    "Advanced certificate/diploma",
    "Associate degree/University transfer",
    "Advanced certificate/diploma",
    "Baccalaureate degree",
    "Certificate",
    "Diploma",
    "Doctorate",
    "Graduate certificate/diploma",
    "Master's degree",
    "Post-degree certificate/diploma",
    "Post-degree certificate/diploma",
    "First professional degree",
    "Graduate certificate/diploma",
    "Apprenticeship",
    "Apprenticeship certificate"
  ),
  COSC_GRAD_STATUS_LGDS_CD = c(
    1,
    1,
    1,
    NA,
    1,
    1,
    NA,
    NA,
    NA,
    1,
    1,
    NA,
    NA,
    NA,
    NA
  )
)


combine_creds <- tibble(
  sort_order = 1:9,
  credential_code = c(
    "3 - ADCT or ADIP",
    "3 - ADGR or UT",
    "3 - CERT",
    "3 - DIPL",
    "3 - PDDP",
    "3 - ADGR or UT",
    "3 - PDCT",
    "3 - PDCT or PDDP",
    "3 - PDCT or PDDP"
  ),
  credential_name = c(
    "Advanced Diploma",
    "Associate Degree",
    "Certificate",
    "Diploma",
    "Post-degree Diploma",
    "University Transfer",
    "Post-degree Certificate",
    "Post-degree Diploma",
    "Post-degree Certificate"
  ),
  reporting_category = c(
    "Advanced Certificate/Advanced Diploma",
    "Associate Degree/University Transfer",
    "Certificate",
    "Diploma",
    "Post-degree Diploma",
    "Associate Degree/University Transfer",
    "Post-degree Certificate",
    "Post-degree Certificate/Post-degree Diploma",
    "Post-degree Certificate/Post-degree Diploma"
  ),
  is_active = c("Yes", "Yes", "Yes", "Yes", NA, "Yes", NA, "Yes", "Yes")
)

stp_dacso_prgm_credential_lookup <- tibble(
  PRGRM_Credential_Awarded = c(
    "ADGR",
    "ADIP",
    "CERT",
    "DIPL",
    "NONE",
    "OTHR",
    "PDCT",
    "PDDP",
    "UT"
  ),
  PRGM_Credential_Awarded_Name = c(
    "Associate Degree",
    "Advanced Diploma",
    "Certificate",
    "Diploma",
    "No credential",
    "Other credential",
    "Post-degree Certificate",
    "Post-degree Diploma",
    "University Transfer"
  ),
  STP_PRGM_Credential_Awarded_Name = c(
    "ASSOCIATE DEGREE",
    "ADVANCED DIPLOMA",
    "CERTIFICATE",
    "DIPLOMA",
    "None- credential code not in STP",
    "Other credential",
    "POST-DEGREE CERTIFICATE",
    "POST-DEGREE DIPLOMA",
    "BACHELORS DEGREE"
  )
)


age_group_lookup <- tibble(
  AgeIndex = 1:9,
  AgeGroup = c(
    "15 to 16",
    "17 to 19",
    "20 to 24",
    "25 to 29",
    "30 to 34",
    "35 to 44",
    "45 to 54",
    "55 to 64",
    "65 to 89"
  ),
  LowerBound = c(15, 17, 20, 25, 30, 35, 45, 55, 65),
  UpperBound = c(16, 19, 24, 29, 34, 44, 54, 64, 89)
)

# these should now be in the R environment
required_tables <- c(
  "t_dacso_data_part_1",
  "credential_non_dup",
  "age_group_lookup",
  "stp_dacso_prgm_credential_lookup",
  "combine_creds",
  "tbl_age",
  "t_pssm_projection_cred_grp",
  "tmp_table_Age"
)

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

na_vals = c("", " ", "(Unspecified)", NA)

# ---- Derive Age at Grad ----
tmp_tbl_age_append_new_years <- tmp_tbl_age_append_new_years |>
  select(
    COSC_STQU_ID = COCI_STQU_ID,
    COSC_SUBM_CD = COCI_SUBM_CD,
    TPID_DATE_OF_BIRTH = BTHDT,
    COSC_ENRL_END_DATE = ENDDT,
    COCI_AGE_AT_SURVEY
  ) |>
  mutate(
    TPID_DATE_OF_BIRTH = lubridate::ym(TPID_DATE_OF_BIRTH, quiet = TRUE), # implicitly convert "bad" dates to NA
    COSC_ENRL_END_DATE = lubridate::ym(COSC_ENRL_END_DATE, quiet = TRUE), # implicitly convert "bad" dates to NA
    COSC_GRAD_CREDENTIAL_DATE = NA_character_,
    Age_At_Grad = NA_real_
  )

#dups made their way in...lookout!
tmp_tbl_age <- tmp_tbl_age |>
  distinct() |>
  rbind(tmp_tbl_age_append_new_years |> distinct())

tmp_tbl_age <- tmp_tbl_age |>
  mutate(
    ref_date = coalesce(COSC_GRAD_CREDENTIAL_DATE, COSC_ENRL_END_DATE),
    year_diff = year(ref_date) - year(TPID_DATE_OF_BIRTH),
    birthday_ref_year = make_date(
      year(ref_date),
      month(TPID_DATE_OF_BIRTH),
      day(TPID_DATE_OF_BIRTH)
    ),
    AGE_AT_GRAD = if_else(
      ref_date < birthday_ref_year,
      year_diff - 1,
      year_diff
    )
  ) |>
  select(-ref_date, -year_diff, -birthday_ref_year)

t_dacso_data_part_1 <- t_dacso_data_part_1 |>
  inner_join(
    tmp_tbl_age |>
      select(COSC_STQU_ID, Age_At_Grad = AGE_AT_GRAD),
    by = c("coci_stqu_id" = "COSC_STQU_ID")
  ) |>
  distinct()

t_dacso_data_part_1_tempselection <- t_dacso_data_part_1 |>
  distinct(
    coci_stqu_id,
    coci_subm_cd,
    coci_age_at_survey,
    age_at_grad = Age_At_Grad,
    cosc_grad_status_lgds_cd_group,
    prgm_credential_awarded,
    prgm_credential_awarded_name,
    pssm_credential,
    pssm_credential_name
  )

# ian used this to pick representitive years (happens later in workflow)
t_dacso_data_part_1_tempselection |>
  filter(
    !is.na(cosc_grad_status_lgds_cd_group),
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  group_by(cosc_grad_status_lgds_cd_group, coci_subm_cd) |>
  summarize(student_count = n(), .groups = "drop") |>
  pivot_wider(
    names_from = coci_subm_cd,
    values_from = student_count,
    values_fill = 0
  )

# ---- Add PEN to Non-Dup table ----
credential_non_dup <- credential_non_dup |>
  left_join(
    stp_credential |>
      select(id = ID, psi_pen = PSI_PEN),
    by = "id"
  )

# ---- DACSO Matching STP Credential ----

# Stage 1: Execute Join, Filter, and Column Initialization
dacso_matching_stp_credential_pen <- t_dacso_data_part_1 |>
  filter(!coci_pen %in% na_vals) |>
  inner_join(
    credential_non_dup,
    by = c("coci_pen" = "psi_pen"),
    relationship = "many-to-many"
  ) |>
  # Replicating GROUP BY logic to ensure distinct records
  distinct(
    coci_stqu_id,
    coci_inst_cd,
    id,
    coci_pen,
    psi_code,
    prgm_credential_awarded,
    prgm_credential_awarded_name,
    pssm_credential,
    pssm_credential_name,
    psi_credential_category,
    outcomes_cred,
    lcp4_cd,
    final_cip_code_4 = FINAL_CIP_CODE_4,
    coci_subm_cd,
    psi_award_school_year,
    cosc_grad_status_lgds_cd_group
  ) |>
  # Initialize the six requested placeholders
  mutate(
    match_credential = NA_character_,
    match_cip_code_4 = NA_character_,
    match_cip_code_2 = NA_character_,
    match_award_school_year = NA_character_,
    match_inst = NA_character_
  )

# Stage 2: Join with Lookup table to populate stp_prgm_credential_awarded_name
dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen |>
  left_join(
    stp_dacso_prgm_credential_lookup |>
      select(
        prgrm_credential_awarded = PRGRM_Credential_Awarded,
        stp_prgm_credential_awarded_name = STP_PRGM_Credential_Awarded_Name,
        prgm_credential_awarded_name = PRGM_Credential_Awarded_Name
      ),
    by = c("prgm_credential_awarded" = "prgrm_credential_awarded")
  )

dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen |>
  mutate(
    match_credential = if_else(
      prgm_credential_awarded_name == psi_credential_category,
      "yes",
      match_credential
    ),
    match_cip_code_4 = if_else(
      lcp4_cd == final_cip_code_4,
      "yes",
      match_cip_code_4
    ),
    match_cip_code_2 = if_else(
      str_sub(lcp4_cd, 1, 2) == str_sub(final_cip_code_4, 1, 2),
      "yes",
      match_cip_code_2
    ),
    match_award_school_year = if_else(
      # Extract digits from 'C_OutcXX' and compare against school year ranges
      (coci_subm_cd == "C_Outc06" &
        psi_award_school_year %in% c("2003/2004", "2004/2005")) |
        (coci_subm_cd == "C_Outc07" &
          psi_award_school_year %in% c("2004/2005", "2005/2006")) |
        (coci_subm_cd == "C_Outc08" &
          psi_award_school_year %in% c("2005/2006", "2006/2007")) |
        (coci_subm_cd == "C_Outc09" &
          psi_award_school_year %in% c("2006/2007", "2007/2008")) |
        (coci_subm_cd == "C_Outc10" &
          psi_award_school_year %in% c("2007/2008", "2008/2009")) |
        (coci_subm_cd == "C_Outc11" &
          psi_award_school_year %in% c("2008/2009", "2009/2010")) |
        (coci_subm_cd == "C_Outc12" &
          psi_award_school_year %in% c("2009/2010", "2010/2011")) |
        (coci_subm_cd == "C_Outc13" &
          psi_award_school_year %in% c("2010/2011", "2011/2012")) |
        (coci_subm_cd == "C_Outc14" &
          psi_award_school_year %in% c("2011/2012", "2012/2013")) |
        (coci_subm_cd == "C_Outc15" &
          psi_award_school_year %in% c("2012/2013", "2013/2014")) |
        (coci_subm_cd == "C_Outc16" &
          psi_award_school_year %in% c("2013/2014", "2014/2015")) |
        (coci_subm_cd == "C_Outc17" &
          psi_award_school_year %in% c("2014/2015", "2015/2016")) |
        (coci_subm_cd == "C_Outc18" &
          psi_award_school_year %in% c("2015/2016", "2016/2017")) |
        (coci_subm_cd == "C_Outc19" &
          psi_award_school_year %in% c("2016/2017", "2017/2018")) |
        (coci_subm_cd == "C_Outc20" &
          psi_award_school_year %in% c("2017/2018", "2018/2019")) |
        (coci_subm_cd == "C_Outc21" &
          psi_award_school_year %in% c("2018/2019", "2019/2020")) |
        (coci_subm_cd == "C_Outc22" &
          psi_award_school_year %in% c("2019/2020", "2020/2021")) |
        (coci_subm_cd == "C_Outc23" &
          psi_award_school_year %in% c("2020/2021", "2021/2022")),
      "yes",
      match_award_school_year
    ),
    match_inst = if_else(
      psi_code == coci_inst_cd |
        (psi_code == "CAP" & coci_inst_cd == "CAPU") |
        (psi_code == "KWAN" & coci_inst_cd == "KPU") |
        (psi_code == "OLA" & coci_inst_cd == "TRU") |
        (psi_code == "MALA" & coci_inst_cd == "VIU") |
        (psi_code == "OUC" & coci_inst_cd == "OKAN") |
        (psi_code == "UCFV" & coci_inst_cd == "UFV") |
        (psi_code == "UCC" & coci_inst_cd == "TRU") |
        (psi_code == "NWCC" & coci_inst_cd == "CMTN"),
      "yes",
      match_inst
    )
  )

match_summary_table <- dacso_matching_stp_credential_pen |>
  group_by(
    match_credential,
    match_cip_code_4,
    match_award_school_year,
    match_inst
  ) |>
  summarize(
    Expr1 = n(),
    .groups = "drop"
  ) |>
  arrange(
    desc(match_credential),
    desc(match_cip_code_4),
    desc(match_award_school_year),
    desc(match_inst)
  )

# Print summary of the matching results for comparison
dbGetQuery(decimal_con, qry06_Match_DACSO_STP_Credential_Summary)

# off a bit in match_credential - investigate why. (involves the following query, run previously).
# Possible culprit is the inenr join on stp_dacso_prgm_credential_lookup
# qry02_Match_DACSO_STP_Credential_PSI_CRED_Category
## GOT TO HERE, all reqd tables are in my schema

# These are considered final matches to STP credential.
dbExecute(
  decimal_con,
  "ALTER TABLE dacso_matching_stp_credential_pen ADD final_consider_a_match nvarchar(10) NULL"
)
dbExecute(
  decimal_con,
  "ALTER TABLE dacso_matching_stp_credential_pen ADD match_all_4_flag nvarchar(10) NULL"
)
dbExecute(decimal_con, qry07_DACSO_STP_Credential_MatchAll4_Flag)

#  Flag records that match on inst, award year, credential, and CIP 2 (but not CIP 4) as final matches too.
dbExecute(decimal_con, qry08_DACSO_STP_Credential_Final_Match_Flag)

# ---- Flag near-completers with earlier or later credential----
dbExecute(decimal_con, qry_Find_NearCompleters_in_STP_Credential_Step1)
dbExecute(
  decimal_con,
  "ALTER TABLE nearcompleters_in_stp_credential_step1 ADD STP_Credential_Awarded_Before_DACSO NVARCHAR(10) NULL"
)
dbExecute(
  decimal_con,
  "ALTER TABLE nearcompleters_in_stp_credential_step1 ADD STP_Credential_Awarded_After_DACSO NVARCHAR(10) NULL"
)
dbExecute(
  decimal_con,
  "ALTER TABLE nearcompleters_in_stp_credential_step1 ADD Has_Multiple_STP_Credentials NVARCHAR(10) NULL"
)
dbExecute(decimal_con, qry_Update_STP_Credential_Awarded_Before_DACSO)
dbExecute(decimal_con, qry_Update_STP_Credential_Awarded_After_DACSO)

dbExecute(decimal_con, qry_make_table_NearCompleters)
dbExecute(
  decimal_con,
  "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_Before_DACSO NVARCHAR(10) NULL"
)
dbExecute(
  decimal_con,
  "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_After_DACSO NVARCHAR(10) NULL"
)
dbExecute(
  decimal_con,
  "ALTER TABLE T_DACSO_NearCompleters ADD Has_Multiple_STP_Credentials NVARCHAR(10) NULL"
)
dbExecute(decimal_con, qry_update_T_DACSO_Near_Completers_step1)
dbExecute(decimal_con, qry_update_T_DACSO_Near_Completers_step2)

# ---- Flag near-completers with multiple credentials----
dbExecute(decimal_con, qry_NearCompleters_With_More_Than_One_Cdtl)
dbExecute(decimal_con, qry_Update_T_NearCompleters_HasMultipleCdtls)

dbExecute(decimal_con, qry_Clean_NearCompleters_MultiCdtls_Step1)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step2)

# Find record with max psi award year
dbExecute(decimal_con, qry_PickMaxYear_step1)
dbExecute(
  decimal_con,
  "ALTER TABLE tmp_NearCompletersWithMultiCredentials_Cleaning ADD Max_Award_School_Year NVARCHAR(10) NULL"
)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step3)

dbExecute(
  decimal_con,
  "ALTER TABLE NearCompleters_in_STP_Credential_Step1 ADD Dup_STQUID_UseThisRecord NVARCHAR(10) NULL"
)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step4)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step5)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step6)
dbExecute(decimal_con, qry_PickMaxYear_Step2)
dbExecute(
  decimal_con,
  "ALTER TABLE tmp_NearCompletersWithMultiCredentials_MaxYearCleaning ADD Final_Record_To_Use NVARCHAR(10) NULL"
)
dbExecute(decimal_con, qry_PickMaxYear_Step3)
dbExecute(
  decimal_con,
  "ALTER TABLE NearCompleters_in_STP_Credential_Step1 ADD Final_Record_To_Use NVARCHAR(10) NULL"
)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step10)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step13)

dbExecute(
  decimal_con,
  "ALTER TABLE DACSO_Matching_STP_Credential_PEN ADD Dup_STQUID_UseThisRecord NVARCHAR(10) NULL"
)
dbExecute(decimal_con, qry_Update_DupStqu_ID_UseThisRecord2)
#dbExecute(decimal_con, "ALTER TABLE NearCompleters_in_STP_Credential_Step1 ADD Final_Record_To_Use NVARCHAR(10) NULL")
dbExecute(decimal_con, qry_Update_Final_Record_To_Use_NearCompletersDups)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step12)
dbExecute(
  decimal_con,
  "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_Before_DACSO_Final NVARCHAR(10) NULL"
)
dbExecute(
  decimal_con,
  "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_After_DACSO_Final NVARCHAR(10) NULL"
)
dbExecute(decimal_con, qry_Update_Final_STP_Cred_Before_or_After_Step1)

dbExecute(
  decimal_con,
  "ALTER TABLE T_DACSO_DATA_Part_1_TempSelection ADD Has_STP_Credential NVARCHAR(10) NULL"
)
dbExecute(
  decimal_con,
  "ALTER TABLE T_DACSO_Data_Part_1 ADD Has_STP_Credential NVARCHAR(10)"
)
dbExecute(decimal_con, qry_update_Has_STP_Credential)

dbExecute(
  decimal_con,
  "ALTER TABLE T_DACSO_Data_Part_1 ADD Grad_Status_Factoring_in_STP nvarchar(2) NULL"
)
dbExecute(
  decimal_con,
  "ALTER TABLE T_DACSO_DATA_Part_1_TempSelection ADD Grad_Status_Factoring_in_STP NVARCHAR(10) NULL"
)
dbExecute(decimal_con, qry_update_Grad_Status_Factoring_in_STP_step1)
dbExecute(decimal_con, qry_update_Grad_Status_Factoring_in_STP_step2)

dbExecute(
  decimal_con,
  "UPDATE T_DACSO_DATA_Part_1 
                        SET Has_STP_Credential = T_DACSO_DATA_Part_1_TempSelection.Has_STP_Credential,
                            Grad_Status_Factoring_In_STP = T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_In_STP
                        FROM T_DACSO_DATA_Part_1 INNER JOIN T_DACSO_DATA_Part_1_TempSelection 
                        ON T_DACSO_DATA_Part_1.COCI_STQU_ID = T_DACSO_DATA_Part_1_TempSelection.COCI_STQU_ID"
)

dbExecute(
  decimal_con,
  "DROP TABLE tmp_NearCompletersWithMultiCredentials_Cleaning"
)
dbExecute(
  decimal_con,
  "DROP TABLE tmp_NearCompletersWithMultiCredentials_MaxYear"
)
dbExecute(
  decimal_con,
  "DROP TABLE tmp_NearCompletersWithMultiCredentials_MaxYearCleaning"
)
dbExecute(decimal_con, "DROP TABLE T_DACSO_NearCompleters")
dbExecute(decimal_con, "DROP TABLE tmp_MaxAwardYear")
dbExecute(
  decimal_con,
  "DROP TABLE tmp_DACSO_NearCompleters_with_Multiple_Cdtls"
)
dbExecute(decimal_con, "DROP TABLE tmp_MaxAwardYearCleaning_MaxID")
dbExecute(decimal_con, "DROP TABLE DACSO_Matching_STP_Credential_PEN")
dbExecute(decimal_con, "DROP TABLE nearcompleters_in_stp_credential_step1")

# ----- Check Near Completers Ratios -----
dbGetQuery(decimal_con, qry99_Investigate_Near_Completes_vs_Graduates_by_Year)
dbGetQuery(decimal_con, qry99_GradStatus_Factoring_in_STP_Credential_by_Year)
dbGetQuery(decimal_con, qry99_GradStatus_byCred_by_Year_Age_At_Grad)
dbGetQuery(
  decimal_con,
  qry99_GradStatus_Factoring_in_STP_byCred_by_Year_Age_At_Grad
)
dbGetQuery(decimal_con, qry_details_of_STP_Credential_Matching)

# Queries are for Excel: C_Outc12_13_14RatiosAgeGradCIP4
#1 (col H in Excel sheet)
dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4)
dbExecute(decimal_con, qry_Make_NearCompleters_CIP4_CombinedCred)
NearCompleters_CIP4_CombinedCred <- dbReadTable(
  decimal_con,
  "NearCompleters_CIP4_CombinedCred"
)
NearCompleters_CIP4_CombinedCred$lcip4_cred <- gsub(
  "-\\s(0|1)\\s",
  "",
  NearCompleters_CIP4_CombinedCred$lcip4_cred
)
NearCompleters_CIP4_CombinedCred <- NearCompleters_CIP4_CombinedCred %>%
  summarise(
    count = sum(CombinedCredCount, na.rm = TRUE),
    .by = c(age_group, lcip4_cred, lcp4_cd)
  )

#2 (col I in Excel sheet)
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_ByCIP4)
dbExecute(decimal_con, qry_Make_NearCompleters_CIP4_With_STP_CombinedCred)
NearCompleters_CIP4_With_STP_CombinedCred <- dbReadTable(
  decimal_con,
  "NearCompleters_CIP4_With_STP_CombinedCred"
)
NearCompleters_CIP4_With_STP_CombinedCred$lcip4_cred <- gsub(
  "-\\s(0|1)\\s",
  "",
  NearCompleters_CIP4_With_STP_CombinedCred$lcip4_cred
)
NearCompleters_CIP4_With_STP_CombinedCred <- NearCompleters_CIP4_With_STP_CombinedCred %>%
  summarise(
    nc_with_earlier_or_later = sum(CombinedCredCount, na.rm = TRUE),
    .by = c(age_group, lcip4_cred, lcp4_cd)
  )


#3 (col K in Excel sheet)
dbExecute(decimal_con, qry99_Completers_agg_factoring_in_STP_Credential_by_CIP4)
dbExecute(
  decimal_con,
  "alter table completersfactoringinstp_cip4 add lcip4_cred_cleaned nvarchar(50) NULL;"
)
dbExecute(
  decimal_con,
  "update completersfactoringinstp_cip4 
                        set lcip4_cred_cleaned = 
                        	CASE WHEN PATINDEX('%1 - %', lcip4_cred) = 1 THEN STUFF(lcip4_cred, 1, 3,'3 -')  
                        	ELSE lcip4_cred
                        	END
                        from completersfactoringinstp_cip4"
)

dbExecute(decimal_con, qry_Make_CompletersFactoringInSTP_CIP4_CombinedCred)
CompletersFactoringInSTP_CIP4_CombinedCred <- dbReadTable(
  decimal_con,
  "CompletersFactoringInSTP_CIP4_CombinedCred"
)
CompletersFactoringInSTP_CIP4_CombinedCred$lcip4_cred <- gsub(
  "-\\s(0|1)\\s",
  "",
  CompletersFactoringInSTP_CIP4_CombinedCred$lcip4_cred_cleaned
)
CompletersFactoringInSTP_CIP4_CombinedCred <- CompletersFactoringInSTP_CIP4_CombinedCred %>%
  summarise(
    completers = sum(CombinedCredCount, na.rm = TRUE),
    .by = c(age_group, lcip4_cred, lcp4_cd)
  )


#4 (col M in Excel sheet)
dbExecute(decimal_con, qry99_Completers_agg_byCIP4)
dbExecute(
  decimal_con,
  "alter table completerscip4 add lcip4_cred_cleaned nvarchar(50) NULL;"
)
dbExecute(
  decimal_con,
  "update completerscip4 
                        set lcip4_cred_cleaned = 
                        	CASE WHEN PATINDEX('%1 - %', lcip4_cred) = 1 THEN STUFF(lcip4_cred, 1, 3,'3 -') 
                        	ELSE lcip4_cred
                        	END
                        from completerscip4"
)

dbExecute(decimal_con, qry_Make_Completers_CIP4_CombinedCred)
Completers_CIP4_CombinedCred <- dbReadTable(
  decimal_con,
  "Completers_CIP4_CombinedCred"
)
Completers_CIP4_CombinedCred$lcip4_cred <- gsub(
  "-\\s(0|1)\\s",
  "",
  Completers_CIP4_CombinedCred$lcip4_cred_cleaned
)
Completers_CIP4_CombinedCred <- Completers_CIP4_CombinedCred %>%
  summarise(
    c_not_factoring_stp = sum(CombinedCredCount, na.rm = TRUE),
    .by = c(age_group, lcip4_cred, lcp4_cd)
  )

T_DACSO_Near_Completers_RatioAgeAtGradCIP4 <- NearCompleters_CIP4_CombinedCred %>%
  left_join(
    NearCompleters_CIP4_With_STP_CombinedCred,
    by = join_by(age_group, lcip4_cred, lcp4_cd)
  ) %>%
  left_join(
    CompletersFactoringInSTP_CIP4_CombinedCred,
    by = join_by(age_group, lcip4_cred, lcp4_cd)
  ) %>%
  left_join(
    Completers_CIP4_CombinedCred,
    by = join_by(age_group, lcip4_cred, lcp4_cd)
  ) %>%
  mutate(across(where(is.numeric), ~ replace_na(., 0))) %>%
  mutate(
    near_completers_stp_cred = count - nc_with_earlier_or_later,
    ratio = near_completers_stp_cred / completers,
    ratio_not_factoring_stp = near_completers_stp_cred / c_not_factoring_stp
  ) %>%
  mutate(across(where(is.double), ~ na_if(., Inf))) %>%
  mutate_all(function(x) ifelse(is.nan(x), NA, x))

dbWriteTable(
  decimal_con,
  name = SQL(glue::glue(
    '"{my_schema}"."T_DACSO_Near_Completers_RatioAgeAtGradCIP4"'
  )),
  T_DACSO_Near_Completers_RatioAgeAtGradCIP4
)
dbExecute(decimal_con, "DROP TABLE NearCompleters_CIP4")
dbExecute(decimal_con, "DROP TABLE NearCompleters_CIP4_with_STP_Credential")
dbExecute(decimal_con, "DROP TABLE completersfactoringinstp_cip4")
dbExecute(decimal_con, "DROP TABLE completerscip4")

# Queries are for Excel: C_Outc12_13_14RatiosByGender
#1: paste to col E
dbExecute(decimal_con, qry99_Near_completes_total_byGender)
Near_completes_total_byGender <- dbReadTable(
  decimal_con,
  "Near_completes_total_byGender"
)
dbExecute(decimal_con, "DROP TABLE Near_completes_total_byGender")

#2: paste to col F
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_by_Gender)
Near_completes_total_with_STP_Credential_by_Gender <- dbReadTable(
  decimal_con,
  "Near_completes_total_with_STP_Credential_by_Gender"
) %>%
  rename("nc_with_early_or_late" = "Count") %>%
  select(-has_stp_credential)
dbExecute(
  decimal_con,
  "DROP TABLE Near_completes_total_with_STP_Credential_by_Gender"
)

#3: looks like paste to H (check)
dbExecute(decimal_con, qry99_Completers_agg_by_gender)
Completers_agg_by_gender <- dbReadTable(
  decimal_con,
  "Completers_agg_by_gender"
) %>%
  rename("completers" = "Count")
dbExecute(decimal_con, "DROP TABLE Completers_agg_by_gender")

ratio.df = Near_completes_total_byGender %>%
  left_join(Near_completes_total_with_STP_Credential_by_Gender) %>%
  left_join(Completers_agg_by_gender) %>%
  rename("gender" = "tpid_lgnd_cd")

# we want the adjusted ratio from column L (or just the normal ratio for nc for this year)
ratio.df <- ratio.df %>%
  mutate(across(where(is.numeric), ~ replace_na(., 0))) %>%
  mutate(n_nc_stp = Count - nc_with_early_or_late) %>%
  mutate(ratio = n_nc_stp / completers)

ratio.df2 <- ratio.df %>%
  filter(
    prgm_credential_awarded_name %in%
      c("Associate Degree", "University Transfer")
  ) %>%
  mutate(prgm_credential_awarded_name = "Associate Degree") %>%
  summarise(
    ratio_adgt = sum(n_nc_stp) / sum(completers),
    .by = c(gender, age_group, prgm_credential_awarded_name)
  )

T_DACSO_Near_Completers_RatioByGender <-
  ratio.df %>%
  left_join(ratio.df2) %>%
  mutate(
    ratio = if_else(
      prgm_credential_awarded_name %in%
        c("Associate Degree", "University Transfer"),
      ratio_adgt,
      ratio
    )
  ) %>%
  mutate(across(where(is.double), ~ na_if(., Inf))) %>%
  mutate_all(function(x) ifelse(is.nan(x), NA, x)) %>%
  select(-ratio_adgt)

dbWriteTable(
  decimal_con,
  name = SQL(glue::glue(
    '"{my_schema}"."T_DACSO_Near_Completers_RatioByGender"'
  )),
  T_DACSO_Near_Completers_RatioByGender
)

# 4. Same as above (3.) but by year - to get historical

# 4.1: paste to col E
dbExecute(decimal_con, qry99_Near_completes_total_byGender_year)
Near_completes_total_byGender_year <- dbReadTable(
  decimal_con,
  "Near_completes_total_byGender_year"
)
dbExecute(decimal_con, "DROP TABLE Near_completes_total_byGender_year")

# 4.2: paste to col F
dbExecute(
  decimal_con,
  qry99_Near_completes_total_with_STP_Credential_by_Gender_year
)
Near_completes_total_with_STP_Credential_by_Gender_year <- dbReadTable(
  decimal_con,
  "Near_completes_total_with_STP_Credential_by_Gender_year"
) %>%
  rename("nc_with_early_or_late" = "Count") %>%
  select(-has_stp_credential)
dbExecute(
  decimal_con,
  "DROP TABLE Near_completes_total_with_STP_Credential_by_Gender_year"
)

# 4.3 get full ratio
dbExecute(decimal_con, qry99_Completers_agg_by_gender_age_year)
Completers_agg_by_gender_age_year <- dbReadTable(
  decimal_con,
  "Completers_agg_by_gender_age_year"
) %>%
  rename("completers" = "Count")
dbExecute(decimal_con, "DROP TABLE Completers_agg_by_gender_age_year")

ratio.df = Near_completes_total_byGender_year %>%
  left_join(Near_completes_total_with_STP_Credential_by_Gender_year) %>%
  left_join(Completers_agg_by_gender_age_year) %>%
  rename("gender" = "tpid_lgnd_cd")

# we want the adjusted ratio from column L (or just the normal ratio for nc for this year)
ratio.df <- ratio.df %>%
  mutate(across(where(is.numeric), ~ replace_na(., 0))) %>%
  mutate(n_nc_stp = Count - nc_with_early_or_late) %>%
  mutate(ratio = n_nc_stp / completers)

ratio.df2 <- ratio.df %>%
  filter(
    prgm_credential_awarded_name %in%
      c("Associate Degree", "University Transfer")
  ) %>%
  mutate(prgm_credential_awarded_name = "Associate Degree") %>%
  summarise(
    ratio_adgt = sum(n_nc_stp) / sum(completers),
    .by = c(gender, age_group, prgm_credential_awarded_name)
  )

# my question here - is this the right year to switch to?
# in lookup table, DACSO data should be sent back by one
T_DACSO_Near_Completers_RatioByGender_year <-
  ratio.df %>%
  left_join(ratio.df2) %>%
  mutate(
    ratio = if_else(
      prgm_credential_awarded_name %in%
        c("Associate Degree", "University Transfer"),
      ratio_adgt,
      ratio
    )
  ) %>%
  mutate(across(where(is.double), ~ na_if(., Inf))) %>%
  mutate_all(function(x) ifelse(is.nan(x), NA, x)) %>%
  select(-ratio_adgt) %>%
  # subtract one here so that it's the first half of the school year
  mutate(
    year = as.numeric(paste0('20', str_sub(coci_subm_cd, 7, 8))) - 1
  )

dbWriteTable(
  decimal_con,
  name = SQL(glue::glue(
    '"{my_schema}"."T_DACSO_Near_Completers_RatioByGender_year"'
  )),
  T_DACSO_Near_Completers_RatioByGender_year
)

# random query
#dbGetQuery(decimal_con, qry99_Near_completes_factoring_in_STP_total)

# ---- TTRAIN tables ----
# This part is not completed  - see documentation
# Note: the first query filters on cosc_grad_status_lgds_cd_group = '3'
dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4_TTRAIN)
dbExecute(
  decimal_con,
  qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN
)
dbExecute(decimal_con, qry99_Near_completes_program_dist_count)

dbExecute(decimal_con, "DROP TABLE Near_completes_total_by_CIP4_TTRAIN")
dbExecute(
  decimal_con,
  "DROP TABLE Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN"
)

# ---- HISTORICAL TTRAIN queries ----
# note: this uses the same intermediate table names as the above, so make sure the 2 drops are performed
dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4_TTRAIN_history)
dbExecute(
  decimal_con,
  qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN_history
)
dbExecute(decimal_con, qry99_Near_completes_program_dist_count_history)

dbExecute(decimal_con, "DROP TABLE Near_completes_total_by_CIP4_TTRAIN")
dbExecute(
  decimal_con,
  "DROP TABLE Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN"
)


# ---- Clean Up ----
# TODO: clean up this section
dbExecute(decimal_con, "DROP TABLE stp_dacso_prgm_credential_lookup")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Age")
dbExecute(decimal_con, "DROP TABLE tbl_Age")
dbExecute(decimal_con, "DROP TABLE AgeGroupLookup")
dbExecute(decimal_con, "DROP TABLE T_DACSO_DATA_Part_1_TempSelection")
dbExecute(decimal_con, "DROP TABLE combine_creds")
dbExecute(decimal_con, "DROP TABLE t_pssm_projection_cred_grp")
dbExecute(decimal_con, "drop table nearcompleters_cip4_combinedcred")
dbExecute(decimal_con, "drop table NearCompleters_CIP4_With_STP_CombinedCred")
dbExecute(decimal_con, "drop table CompletersFactoringInSTP_CIP4_CombinedCred")
dbExecute(decimal_con, "drop table Completers_CIP4_CombinedCred")

# ---- Keep for program projections ----
dbExistsTable(decimal_con, "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN")
dbExistsTable(decimal_con, "T_DACSO_Near_Completers_RatioAgeAtGradCIP4")
dbExistsTable(decimal_con, "T_DACSO_Near_Completers_RatioByGender")
