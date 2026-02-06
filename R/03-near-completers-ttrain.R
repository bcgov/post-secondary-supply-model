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

library(tidyverse)
library(DBI)
library(odbc)
library(config)

# ---- Configure LAN and file paths ----
#PR Notes for this section:

lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to database ----
#PR Notes for this section:
db_config <- config::get("decimal")
decimal_con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# ---- Data Requirements and SQL Definitons ----
# PR Notes for this section:
# 1) several key tables were made in earlier scripts that I assume will be
# written back to decimal for intermediate storage (between script processes).  For the PR, you may need
# to bring them in from the master schema into your schema before running this code.  You'll need the following key
# tables in decimal: STP_Credential, t_dacso_data_part_1,credential_non_dup
# 2) additionally, for the PR only, you'll need a few lookup tables in decimal.  They are on the LAN but I hard-coded
# them here so you can write them to decimal if you like. The LAN versions are in development/csv/gh-source/lookups. I tried to keep the upper/lower case the same as SQL
# so sometimes you'll see a column with mixed types (we can change later).  The exception is t_pssm_projection_cred_grp; I
# updated the hard-coded values in one column so they were comparable across datasets in R.  SQL Server is not case-sensitive
# so those queries should run as expected, the only implication is if you load this table from LAN the R code will be wrong.
# 3) tmp_tbl_age is oddly designed.  Historically, the analyst appended new data onto old data
# but now we should be able to work with SO team to transfer one csv (possibly split across years) to the LAN.
# For development, we currently append new years to historical data (one of the datasets contains duplicates); we write
# the combined table to decimal along with tmp_tbl_age to get the queries to run.

#source("./sql/03-near-completers/near-completers-investigation-ttrain.R") # remove after development
#source("./sql/03-near-completers/dacso-near-completers.R") # remove after this refactor

dbExecute(
  decimal_con,
  SQL(glue::glue(
    'ALTER TABLE "{my_schema}"."t_dacso_data_part_1" DROP COLUMN Age_At_Grad, Has_STP_Credential, Grad_Status_Factoring_in_STP;'
  ))
) # remove after this refactor

t_dacso_data_part_1 <- dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."t_dacso_data_part_1"'))
)

dbExecute(
  decimal_con,
  SQL(glue::glue(
    'ALTER TABLE "{my_schema}"."Credential_Non_Dup" DROP COLUMN PSI_PEN;'
  ))
) # remove after this refactor

credential_non_dup <- dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"'))
)

stp_credential <- dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."STP_Credential"'))
)

years <- 2018:2023

tmp_tbl_age_append_new_years <- years |>
  purrr::map_dfr(
    ~ {
      file_path <- glue::glue(
        "{lan}/data/student-outcomes/csv/so-provision/qry_make_tmp_table_Age_step1_{.x}.csv"
      )
      read_csv(file_path, col_types = "dcdcd")
    }
  ) |>
  distinct() # duplicates in this data

#dbWriteTable(
#  decimal_con,
#  SQL(glue::glue('"{my_schema}"."tmp_tbl_Age_AppendNewYears"')),
#  tmp_tbl_age_append_new_years
#)

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
  ) |>
  distinct() # no duplicates in this data, but just in case.

#dbWriteTable(
#  decimal_con,
#  SQL(glue::glue('"{my_schema}"."tmp_tbl_Age"')),
#  tmp_tbl_age
#)

# lookups
tbl_age <- tibble(
  Age = 0:150
) |>
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
    "Advanced Certificate",
    "Associate Degree",
    "Advanced Diploma",
    "Bachelors Degree",
    "Certificate",
    "Diploma",
    "Doctorate",
    "Graduate Certificate",
    "Masters Degree",
    "Post-Degree Certificate",
    "Post-Degree Diploma",
    "First Professional Degree",
    "Graduate Diploma",
    "Apprappr",
    "Apprcert"
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
    "Post-Degree certificate/diploma",
    "Post-Degree certificate/diploma",
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
  id = 1:9,
  combined_cred = c(
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
  prgm_credential_awarded_name = c(
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
  combined_cred_name = c(
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
  use_in_pssm_2017_18 = c(
    "Yes",
    "Yes",
    "Yes",
    "Yes",
    NA,
    "Yes",
    NA,
    "Yes",
    "Yes"
  )
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
    "Post-Degree Certificate",
    "Post-Degree Diploma",
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
dbWriteTable(
  decimal_con,
  "stp_dacso_prgm_credential_lookup",
  stp_dacso_prgm_credential_lookup,
  overwrite = TRUE
)


age_group_lookup <- tibble(
  Age_Index = 1:9,
  Age_Group = c(
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
  Lower_Bound = c(15, 17, 20, 25, 30, 35, 45, 55, 65),
  Upper_Bound = c(16, 19, 24, 29, 34, 44, 54, 64, 89)
)

credential_rank <- tribble(
  ~PSI_CREDENTIAL_CATEGORY    , ~RANK ,
  "ADVANCED CERTIFICATE"      ,    10 ,
  "ADVANCED DIPLOMA"          ,     9 ,
  "APPRENTICESHIP"            ,    14 ,
  "ASSOCIATE DEGREE"          ,    11 ,
  "BACHELORS DEGREE"          ,     8 ,
  "CERTIFICATE"               ,    13 ,
  "DIPLOMA"                   ,    12 ,
  "DOCTORATE"                 ,     1 ,
  "FIRST PROFESSIONAL DEGREE" ,     7 ,
  "GRADUATE CERTIFICATE"      ,     4 ,
  "GRADUATE DIPLOMA"          ,     3 ,
  "MASTERS DEGREE"            ,     2 ,
  "POST-DEGREE CERTIFICATE"   ,     6 ,
  "POST-DEGREE DIPLOMA"       ,     5
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
  "tmp_tbl_age",
  "credential_rank"
)

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

na_vals = c("", " ", "(Unspecified)", NA)

#remove after refactor
#dbWriteTable(decimal_con, "tmp_tbl_age", tmp_tbl_age, overwrite = TRUE)
#dbWriteTable(decimal_con, "tmp_tbl_age_append_new_years", tmp_tbl_age_append_new_years, overwrite = TRUE)
#dbWriteTable(decimal_con, "stp_dacso_prgm_credential_lookup", stp_dacso_prgm_credential_lookup, overwrite = TRUE)
#dbWriteTable(decimal_con, "combine_creds", combine_creds, overwrite = TRUE)
#dbWriteTable(decimal_con, "credential_rank", credential_rank, overwrite = TRUE)
#dbWriteTable(decimal_con, "tbl_age", tbl_age, overwrite = TRUE)
#dbWriteTable(decimal_con, "t_pssm_projection_cred_grp", t_pssm_projection_cred_grp, overwrite = TRUE)
#dbWriteTable(decimal_con, "AgeGroupLookup", age_group_lookup)

# ---- Derive Age at Grad ----
# replicates lines 69:87 (main branch)
# testing: t_dacso_data_part_1_tempselection vs output of query at line 87 (main branch)
## Notes:
# 1) near the end of this section we create a new table from the t_dacso dataset.
# The new table will be used later in the workflow; I think it is erronesouly
# placed here in this section but perfect for testing the code in this section.
# I believe it is a "Check" used for a manual decision: used to pick representitive years
# from which to calculate the completers to near-completers ratio.
# I suspect the analyst had other insight to draw upon when making this decsion.

# combine all age data from previous and new years
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
    Age_At_Grad = NA_real_ # remove after this refactor
  )

tmp_tbl_age <- tmp_tbl_age |>
  rbind(tmp_tbl_age_append_new_years) |>
  select(-Age_At_Grad) |> # remove after this refactor
  distinct() # just in case

# derive age at grad variable
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

# bring age at grad into t_dacso dataset
t_dacso_data_part_1 <- t_dacso_data_part_1 |>
  inner_join(
    tmp_tbl_age |>
      select(COSC_STQU_ID, Age_At_Grad = AGE_AT_GRAD),
    by = c("coci_stqu_id" = "COSC_STQU_ID")
  ) |>
  distinct() # just in case

# this table isn't really relevent to this section (see notes above)
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

t_dacso_data_part_1_tempselection |>
  filter(
    !is.na(cosc_grad_status_lgds_cd_group),
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  summarize(
    student_count = n(),
    .by = c("cosc_grad_status_lgds_cd_group", "coci_subm_cd")
  ) |>
  pivot_wider(
    names_from = coci_subm_cd,
    values_from = student_count,
    values_fill = 0
  )

# ---- Add PEN to Non-Dup table ----
# replicates lines 90:101 (main branch)
# testing: NA
## Notes:
#  1) (from main branch) "Move to earlier workflow - 02 series.
#   This updates credential non-dup in current schema only".  We should
# confirm this.
credential_non_dup <- credential_non_dup |>
  left_join(
    stp_credential |>
      select(id = ID, psi_pen = PSI_PEN),
    by = "id"
  )

# ---- DACSO Matching STP Credential ----
# replicates lines 90:101 (main branch)
# testing:at end of section, compare dacso_matching_stp_credential_pen in R vs dacso_matching_stp_credential_pen in SQL
# testing: at end of section, compare qry06 at line 124 (main) vs match_summary_table in R
## Notes:
dacso_matching_stp_credential_pen <- t_dacso_data_part_1 |>
  filter(!coci_pen %in% na_vals) |>
  inner_join(
    credential_non_dup,
    by = c("coci_pen" = "psi_pen"),
    relationship = "many-to-many"
  ) |>
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
  )

# Stage 2: Join with Lookup table to populate stp_prgm_credential_awarded_name
dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen |>
  left_join(
    stp_dacso_prgm_credential_lookup |>
      select(
        prgrm_credential_awarded = PRGRM_Credential_Awarded,
        stp_prgm_credential_awarded_name = STP_PRGM_Credential_Awarded_Name
      ),
    by = c("prgm_credential_awarded" = "prgrm_credential_awarded")
  )

dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen |>
  mutate(
    match_credential = if_else(
      str_equal(
        prgm_credential_awarded_name,
        psi_credential_category,
        ignore_case = TRUE
      ),
      "yes",
      NA_character_
    ),
    match_cip_code_4 = if_else(
      lcp4_cd == final_cip_code_4,
      "yes",
      NA_character_
    ),
    match_cip_code_2 = if_else(
      str_sub(lcp4_cd, 1, 2) == str_sub(final_cip_code_4, 1, 2),
      "Yes", #align with SQL version which capitalizes this
      NA_character_
    ),
    suffix = as.numeric(str_extract(coci_subm_cd, "\\d+$")),
    base_year = 1997 + suffix,
    psi_start_year = as.numeric(str_sub(psi_award_school_year, 1, 4)),
    match_award_school_year = case_when(
      psi_start_year == base_year ~ "yes",
      psi_start_year == base_year + 1 ~ "yes",
      TRUE ~ NA_character_
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
      NA_character_
    )
  ) |>
  select(-suffix, -base_year, -psi_start_year)

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

# These are considered final matches to STP credential
dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen |>
  mutate(
    match_all_4_flag = if_else(
      if_all(
        c(
          match_credential,
          match_cip_code_4,
          match_award_school_year,
          match_inst
        ),
        ~ .x == "yes"
      ),
      "yes",
      NA_character_
    ),

    # Step 2: Determine final match status based on Step 1 OR the CIP-2 fallback
    final_consider_a_match = case_when(
      match_all_4_flag == "yes" ~ "yes",

      # Fallback: CIP-4 is missing, but CIP-2 and everything else matches
      match_credential == "yes" &
        match_cip_code_2 == "Yes" &
        is.na(match_cip_code_4) &
        match_award_school_year == "yes" &
        match_inst == "yes" ~ "yes",

      TRUE ~ NA_character_
    )
  )

# ---- Flag near-completers with earlier or later credential----
# replicates lines 135:147  (main branch)
# testing: at end of section, compare t_dacso_nearcompleters in R vs t_dacso_nearcompleters in SQL

#identify "Near-Completers" and join their survey data with their credential records.
nearcompleters_in_stp_credential_step1 <- t_dacso_data_part_1 |>
  select(
    coci_stqu_id,
    coci_subm_cd,
    Age_At_Grad,
    prgm_credential_awarded,
    prgm_credential_awarded_name,
    pssm_credential,
    pssm_credential_name,
    lcp4_cd,
    cosc_grad_status_lgds_cd_group
  ) |>
  # 1. Filter for specific outcome codes, age range, and status '3'
  filter(
    coci_subm_cd %in% paste0("C_Outc", sprintf("%02d", 7:23)),
    Age_At_Grad >= 17,
    Age_At_Grad <= 64,
    cosc_grad_status_lgds_cd_group == "3"
  ) |>
  inner_join(
    dacso_matching_stp_credential_pen |>
      select(
        coci_stqu_id,
        id,
        coci_pen,
        coci_inst_cd,
        psi_code,
        prgm_credential_awarded,
        prgm_credential_awarded_name,
        stp_prgm_credential_awarded_name,
        pssm_credential,
        pssm_credential_name,
        psi_credential_category,
        outcomes_cred,
        final_cip_code_4,
        psi_award_school_year,
        match_award_school_year,
        match_inst,
        final_consider_a_match,
        match_all_4_flag,
        match_credential,
        match_cip_code_4,
        match_cip_code_2
      ),
    by = "coci_stqu_id",
    suffix = c("_dacso", "_stp") # Handles duplicate column names automatically
  ) |>
  rename(
    dacso_prgm_credential_awarded = prgm_credential_awarded_dacso,
    dacso_prgm_credential_awarded_name = prgm_credential_awarded_name_dacso,
    dacso_pssm_credential = pssm_credential_dacso,
    dacso_pssm_credential_name = pssm_credential_name_dacso
  )


# Logic: The max 'Before' award year is always (Survey Value + 1998)
# e.g., Outcome 07 + 1998 = 2005. Any award <= 2005 is 'Before'.
nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  mutate(
    survey_val = as.numeric(str_extract(coci_subm_cd, "\\d+")),
    award_year_start = as.numeric(str_sub(psi_award_school_year, 1, 4)),
    stp_credential_awarded_before_dacso = if_else(
      award_year_start <= (survey_val + 1998),
      "Yes",
      NA_character_
    )
  ) |>
  select(-survey_val, -award_year_start)

nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  mutate(
    stp_credential_awarded_after_dacso = if_else(
      is.na(stp_credential_awarded_before_dacso),
      "Yes",
      NA_character_
    )
  )


t_dacso_nearcompleters <- t_dacso_data_part_1 |>
  # 1. Filter for the core population
  filter(
    cosc_grad_status_lgds_cd_group == "3",
    Age_At_Grad >= 17,
    Age_At_Grad <= 64
  ) |>
  # 2. Join with the full credential list (this creates multiple rows temporarily)
  left_join(
    nearcompleters_in_stp_credential_step1 |>
      select(
        coci_stqu_id,
        stp_credential_awarded_before_dacso,
        stp_credential_awarded_after_dacso
      ),
    by = "coci_stqu_id"
  ) |>
  group_by(across(c(
    coci_stqu_id,
    coci_subm_cd,
    Age_At_Grad,
    cosc_grad_status_lgds_cd_group,
    prgm_credential_awarded,
    prgm_credential_awarded_name,
    pssm_credential,
    pssm_credential_name
  ))) |>
  summarize(
    stp_credential_awarded_before_dacso = if_else(
      any(stp_credential_awarded_before_dacso == "Yes", na.rm = TRUE),
      "Yes",
      NA_character_
    ),
    stp_credential_awarded_after_dacso = if_else(
      any(stp_credential_awarded_after_dacso == "Yes", na.rm = TRUE),
      "Yes",
      NA_character_
    ),
    .groups = "drop"
  )

# ---- Flag near-completers with multiple credentials----
# replicates lines 150:204 (main branch)
# testing: at end of section, compare t_dacso_data_part_1 in R vs t_dacso_data_part_1 in SQL.  This one table
# should carry all of the flags we create in this section.
# Notes: there is quite a bit we can do to condense this code. Leaving for now but noting that
# the logic is similar to prior workflows; when faced with the problem of picking one label in a column
# we often use a window-function approach, removing the need for intermediate vectors and multiple steps.

# Update the main matching table with 'Multiple' and 'UseThis' flags
nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  group_by(coci_stqu_id) |>
  mutate(
    has_multiple_stp_credentials = if_else(n() > 1, "Yes", NA_character_)
  ) |>
  ungroup()

ids <- nearcompleters_in_stp_credential_step1 |>
  filter(has_multiple_stp_credentials == "Yes") |>
  pull(coci_stqu_id)

# Update the final reporting table with the 'Multiple' flag
t_dacso_nearcompleters <- t_dacso_nearcompleters |>
  mutate(
    has_multiple_stp_credentials = if_else(
      coci_stqu_id %in% ids,
      "Yes",
      NA_character_
    )
  )

nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  group_by(coci_stqu_id) |>
  mutate(
    Max_Award_School_Year = if_else(
      psi_award_school_year == max(psi_award_school_year, na.rm = TRUE),
      "Yes",
      NA_character_
    )
  ) |>
  ungroup()

nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  mutate(
    dup_stquid_usethisrecord = if_else(
      has_multiple_stp_credentials == "Yes" &
        Max_Award_School_Year == "Yes",
      "Yes",
      NA_character_
    )
  )


# This replaces Steps 5, 6, PickMaxYear 2, and PickMaxYear 3
final_winners <- nearcompleters_in_stp_credential_step1 |>
  filter(dup_stquid_usethisrecord == "Yes") |>
  group_by(coci_stqu_id) |>
  filter(n() > 1) |>
  filter(id == max(id, na.rm = TRUE)) |>
  mutate(final_record_to_use = "Yes") |>
  ungroup() |>
  select(
    id,
    coci_stqu_id,
    final_record_to_use,
    stp_credential_awarded_before_dacso,
    stp_credential_awarded_after_dacso
  )

# 2. Update the main matching table (Replaces Step 10)
nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  left_join(
    final_winners |> select(id, coci_stqu_id, final_record_to_use),
    by = c("id", "coci_stqu_id")
  )

# 3. Update the final reporting table (Replaces Step 13)
t_dacso_nearcompleters <- t_dacso_nearcompleters |>
  left_join(
    final_winners |>
      select(
        coci_stqu_id,
        stp_credential_awarded_before_dacso_update = stp_credential_awarded_before_dacso,
        stp_credential_awarded_after_dacso_update = stp_credential_awarded_after_dacso,
      ),
    by = "coci_stqu_id"
  ) |>
  mutate(
    stp_credential_awarded_before_dacso = coalesce(
      stp_credential_awarded_before_dacso,
      stp_credential_awarded_before_dacso_update
    ),
    stp_credential_awarded_after_dacso = coalesce(
      stp_credential_awarded_after_dacso,
      stp_credential_awarded_after_dacso_update
    )
  ) |>
  select(
    -stp_credential_awarded_before_dacso_update,
    -stp_credential_awarded_after_dacso_update
  )

# off by a small handful here in "t_dacso_nearcompleters.stp_credential_awarded_before_dacso" before column
#  (I there where there were multiple awards in a single year)
#dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step13)

# Update the primary matching table with the finalized 'UseThisRecord' flag
dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen |>
  left_join(
    final_winners |> select(id, coci_stqu_id, final_record_to_use),
    by = c("id", "coci_stqu_id")
  ) |>
  rename(dup_stquid_usethisrecord = final_record_to_use)

nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  mutate(
    final_record_to_use = if_else(
      is.na(final_record_to_use) & is.na(has_multiple_stp_credentials),
      "Yes",
      final_record_to_use
    )
  )

t_dacso_nearcompleters <- t_dacso_nearcompleters |>
  left_join(
    nearcompleters_in_stp_credential_step1 |>
      filter(final_record_to_use == "yes") |>
      select(
        coci_stqu_id,
        stp_credential_awarded_before_dacso_final = stp_credential_awarded_before_dacso,
        stp_credential_awarded_after_dacso_final = stp_credential_awarded_after_dacso
      ),
    by = "coci_stqu_id"
  )

t_dacso_data_part_1_tempselection <- t_dacso_data_part_1_tempselection |>
  left_join(
    t_dacso_nearcompleters |>
      filter(
        stp_credential_awarded_before_dacso == "Yes" |
          stp_credential_awarded_after_dacso == "Yes"
      ) |>
      select(coci_stqu_id) |>
      mutate(has_stp_credential = "Yes"),
    by = "coci_stqu_id"
  ) |>
  mutate(grad_status_factoring_in_stp = cosc_grad_status_lgds_cd_group)

t_dacso_data_part_1_tempselection <- t_dacso_data_part_1_tempselection |>
  mutate(
    grad_status_factoring_in_stp = as.character(if_else(
      (grad_status_factoring_in_stp == 3 &
        !is.na(has_stp_credential) &
        has_stp_credential == "Yes"),
      1,
      grad_status_factoring_in_stp
    ))
  )

t_dacso_data_part_1 <- t_dacso_data_part_1 |>
  left_join(
    t_dacso_data_part_1_tempselection |>
      select(
        coci_stqu_id,
        has_stp_credential = has_stp_credential,
        grad_status_factoring_in_stp = grad_status_factoring_in_stp
      ),
    by = "coci_stqu_id"
  )

# ----- Check Near Completers Ratios -----
# replicates lines 207:211 (main branch)
# testing: check each code chunk against each query on main branch.  Each SQL
# query generates output in R.

t_dacso_data_part_1_tempselection |>
  filter(
    !is.na(cosc_grad_status_lgds_cd_group),
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  group_by(cosc_grad_status_lgds_cd_group, coci_subm_cd) |>
  summarise(count = n(), .groups = "drop") |>
  pivot_wider(
    names_from = coci_subm_cd,
    values_from = count,
    id_cols = cosc_grad_status_lgds_cd_group
  ) |>
  mutate(across(starts_with("C_Outc"), ~ replace_na(., 0)))

t_dacso_data_part_1_tempselection |>
  filter(
    !is.na(grad_status_factoring_in_stp),
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  count(grad_status_factoring_in_stp, coci_subm_cd) |>
  pivot_wider(
    names_from = coci_subm_cd,
    values_from = n,
    values_fill = 0
  ) |>
  arrange(grad_status_factoring_in_stp)

t_dacso_data_part_1_tempselection |>
  filter(
    !is.na(cosc_grad_status_lgds_cd_group),
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  group_by(
    pssm_credential,
    pssm_credential_name,
    cosc_grad_status_lgds_cd_group,
    coci_subm_cd
  ) |>
  summarise(count = n(), .groups = "drop") |>
  pivot_wider(
    names_from = coci_subm_cd,
    values_from = count,
    values_fill = 0
  ) |>
  arrange(pssm_credential, cosc_grad_status_lgds_cd_group)

t_dacso_data_part_1_tempselection |>
  filter(
    !is.na(grad_status_factoring_in_stp),
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  group_by(
    pssm_credential,
    pssm_credential_name,
    grad_status_factoring_in_stp,
    coci_subm_cd
  ) |>
  summarise(count = n(), .groups = "drop") |>
  pivot_wider(
    names_from = coci_subm_cd,
    values_from = count,
    values_fill = 0
  ) |>
  arrange(pssm_credential, grad_status_factoring_in_stp)

t_dacso_data_part_1_tempselection |>
  filter(
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  count(
    coci_subm_cd,
    cosc_grad_status_lgds_cd_group,
    grad_status_factoring_in_stp,
    name = "expr1"
  ) |>
  arrange(coci_subm_cd, cosc_grad_status_lgds_cd_group)


# ----------------------- Transferred From Excel Sheet -----------------------

# ----------------------- Age At Grad by CIP4 Ratios -----------------------
# replicates lines 214:279 (main branch)
# testing: 1) run the code from here to to line 1115 (refactor branch).  Run the queries from line 215 to 279 (main branch).
# There will be two comparable tables in your R environment:  T_DACSO_Near_Completers_RatioAgeAtGradCIP4 and t_dacso_nearcompleters_ratioageatgradcip4.
# Notes:
# 1) age_group_lookup colnames are set to lower case here to align with the SQL queries from here to the end of script.
# 2) col H: we appear to be using different age groups from the original.  However,
# there is code to handle this in 04-graduate-projections.R so we get to decide which way is "right" later.
# 3) The section replicates queries linked to this Excel workbook: 2017-2018\Development\Graduate Model\Near Completers.
# Each section of code is labeled with a reference to the sheet name and column of data - these values will not be identical (we used different years)
# but should be similar.
# 4) AgeGroupLookup columns in SQL follow a different naming convention.  I've changed the colnames here - I'm thinking we can remove them
# when we figure this out.
names(age_group_lookup) <- tolower(names(age_group_lookup)) # remove (or move) after refactor
dbGetQuery(
  decimal_con,
  "EXEC sp_rename 'AgeGroupLookup.LowerBound', 'Lower_Bound', 'COLUMN';"
)
dbGetQuery(
  decimal_con,
  "EXEC sp_rename 'AgeGroupLookup.UpperBound', 'Upper_Bound', 'COLUMN';"
)
dbGetQuery(
  decimal_con,
  "EXEC sp_rename 'AgeGroupLookup.AgeGroup', 'Age_Group', 'COLUMN';"
)


base <- t_dacso_data_part_1 |>
  select(-age_group) |>
  filter(
    coci_subm_cd %in% c("C_Outc19", "C_Outc20")
  ) |>
  inner_join(
    age_group_lookup,
    by = join_by(Age_At_Grad >= lower_bound, Age_At_Grad <= upper_bound)
  ) |>
  left_join(
    credential_rank,
    by = c("prgm_credential_awarded_name" = "PSI_CREDENTIAL_CATEGORY")
  ) |>
  inner_join(
    combine_creds |>
      filter(use_in_pssm_2017_18 == "Yes"),
    by = "prgm_credential_awarded_name"
  ) |>
  mutate(
    lcip4_cred = gsub("-\\s(0|1)\\s", "", LCIP4_CRED),
    lcip4_cred_cleaned = if_else(
      str_detect(lcip4_cred, "^1 - "),
      str_replace(lcip4_cred, "^1 - ", "3 - "),
      lcip4_cred
    )
  )

#1 (col H in Excel sheet C_Outc12_13_14RatiosAgeGradCIP4)
nearcompleters_cip4_combinedcred <- base |>
  filter(cosc_grad_status_lgds_cd_group == "3") |>
  count(
    age_group,
    combined_cred_name,
    lcip4_cred,
    lcp4_cd,
    lcp4_cip_4digits_name,
    name = "count"
  )

#2 (col I in Excel sheet)
near_completers_cip4_with_stp_combined_cred <- base |>
  filter(has_stp_credential == "Yes") |>
  inner_join(
    t_dacso_data_part_1_tempselection |> distinct(coci_stqu_id),
    by = "coci_stqu_id"
  ) |>
  count(age_group, lcip4_cred, lcp4_cd, name = "nc_with_earlier_or_later")

#3 (col K in Excel sheet)
completers_factoring_in_stp_cip4_combined_cred <- base |>
  filter(
    grad_status_factoring_in_stp == "1",
    Age_At_Grad >= 17,
    Age_At_Grad <= 64
  ) |>
  count(age_group, lcip4_cred_cleaned, lcp4_cd, name = "completers") |>
  rename(lcip4_cred = lcip4_cred_cleaned)

#4 (col M in Excel sheet)
completers_cip4_combined_cred <- base |>
  filter(
    cosc_grad_status_lgds_cd_group == "1",
    Age_At_Grad >= 17,
    Age_At_Grad <= 64
  ) |>
  count(age_group, lcip4_cred_cleaned, lcp4_cd, name = "c_not_factoring_stp") |>
  rename(lcip4_cred = lcip4_cred_cleaned)

# Make final ratios ----
t_dacso_nearcompleters_ratioageatgradcip4 <-
  nearcompleters_cip4_combinedcred |>
  left_join(
    near_completers_cip4_with_stp_combined_cred,
    by = join_by(age_group, lcip4_cred, lcp4_cd)
  ) |>
  left_join(
    completers_factoring_in_stp_cip4_combined_cred,
    by = join_by(age_group, lcip4_cred, lcp4_cd)
  ) |>
  left_join(
    completers_cip4_combined_cred,
    by = join_by(age_group, lcip4_cred, lcp4_cd)
  ) |>
  mutate(across(where(is.numeric), ~ replace_na(., 0))) |>
  mutate(
    near_completers_stp_cred = count - nc_with_earlier_or_later,
    ratio = near_completers_stp_cred / completers,
    ratio_not_factoring_stp = near_completers_stp_cred / c_not_factoring_stp
  ) |>
  mutate(across(where(is.double), ~ na_if(., Inf))) |>
  mutate_all(function(x) ifelse(is.nan(x), NA, x))

# ----------------------- Gender by CIP4 Ratios -----------------------
# replicates lines 281:324 (main branch)
# testing:
# 1) run the code from here to to line 1223 (refactor branch).  Run the queries from line 281 to 324 (main branch).
# There will be two comparable tables in your R environment:  T_DACSO_Near_Completers_RatioByGender and t_dacso_near_completers_ratio_by_gender.
# 2) run the code from line 1227 to line 1327 (refactor branch).  Run the queries from line 329 to 374 (main branch).
# There will be two comparable tables in your R environment:  T_DACSO_Near_Completers_RatioByGender_year and t_dacso_near_completers_ratio_by_gender_year.
# Notes:
# 1) See notes for above section, this code replicates queries linked to the same Excel workbook, different sheet.

# Queries are for Excel: C_Outc12_13_14RatiosByGender
base <- t_dacso_data_part_1 |>
  select(-age_group, -has_stp_credential) |>
  rename(age_at_grad = Age_At_Grad) |>
  filter(
    coci_subm_cd %in% c("C_Outc19", "C_Outc20"),
    tpid_lgnd_cd != "0"
  ) |>
  inner_join(
    age_group_lookup,
    by = join_by(age_at_grad >= lower_bound, age_at_grad <= upper_bound)
  ) |>
  left_join(
    credential_rank,
    by = c("prgm_credential_awarded_name" = "PSI_CREDENTIAL_CATEGORY")
  )

# 1: paste to col E (C_Outc12_13_14RatiosByGender)
near_completes_total_by_gender <- base |>
  filter(
    cosc_grad_status_lgds_cd_group == "3"
  ) |>
  count(
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "count"
  )

#2: paste to col F (C_Outc12_13_14RatiosByGender)
near_completes_total_with_stp_by_gender <- base |>
  inner_join(
    t_dacso_data_part_1_tempselection |>
      distinct(coci_stqu_id, has_stp_credential),
    by = "coci_stqu_id"
  ) |>
  filter(
    has_stp_credential == "Yes"
  ) |>
  count(
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "nc_with_early_or_late"
  )

#3: looks like paste to H (C_Outc12_13_14RatiosByGender) (Need to check this)
completers_agg_by_gender <- base |>
  filter(
    cosc_grad_status_lgds_cd_group == "1",
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  count(
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "completers"
  )

ratio.df = near_completes_total_by_gender |>
  left_join(near_completes_total_with_stp_by_gender) |>
  left_join(completers_agg_by_gender) |>
  rename("gender" = "tpid_lgnd_cd")

# we want the adjusted ratio from column L (C_Outc12_13_14RatiosByGender)
# (alternatively just the normal ratio for nc for this year)
ratio.df <- ratio.df |>
  mutate(across(where(is.numeric), ~ replace_na(., 0))) |>
  mutate(n_nc_stp = count - nc_with_early_or_late) |>
  mutate(ratio = n_nc_stp / completers)

ratio.df2 <- ratio.df |>
  filter(
    prgm_credential_awarded_name %in%
      c("Associate Degree", "University Transfer")
  ) |>
  mutate(prgm_credential_awarded_name = "Associate Degree") |>
  summarise(
    ratio_adgt = sum(n_nc_stp) / sum(completers),
    .by = c(gender, age_group, prgm_credential_awarded_name)
  )

t_dacso_near_completers_ratio_by_gender <-
  ratio.df |>
  left_join(ratio.df2) |>
  mutate(
    ratio = if_else(
      prgm_credential_awarded_name %in%
        c("Associate Degree", "University Transfer"),
      ratio_adgt,
      ratio
    )
  ) |>
  mutate(across(where(is.double), ~ na_if(., Inf))) |>
  mutate_all(function(x) ifelse(is.nan(x), NA, x)) |>
  select(-ratio_adgt)


# 4.1: paste to col E (C_Outc12_13_14RatiosByGender)
base <- t_dacso_data_part_1 |>
  rename(age_at_grad = Age_At_Grad) |>
  select(-age_group, -has_stp_credential) |>
  filter(tpid_lgnd_cd != "0") |>
  inner_join(
    age_group_lookup,
    by = join_by(age_at_grad >= lower_bound, age_at_grad <= upper_bound)
  ) |>
  left_join(
    credential_rank,
    by = c("prgm_credential_awarded_name" = "PSI_CREDENTIAL_CATEGORY")
  )

near_completes_total_by_gender_year <- base |>
  filter(
    cosc_grad_status_lgds_cd_group == "3"
  ) |>
  count(
    coci_subm_cd,
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "count"
  )

# 4.2: paste to col F (C_Outc12_13_14RatiosByGender)
near_completes_total_with_stp_by_gender_year <- base |>
  inner_join(
    t_dacso_data_part_1_tempselection |>
      select(coci_stqu_id, has_stp_credential),
    by = "coci_stqu_id"
  ) |>
  filter(
    has_stp_credential == "Yes"
  ) |>
  count(
    coci_subm_cd,
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "nc_with_early_or_late"
  )

# 4.3 get full ratio (C_Outc12_13_14RatiosByGender)
completers_agg_by_gender_age_year <- base |>
  filter(
    cosc_grad_status_lgds_cd_group == "1",
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  count(
    coci_subm_cd,
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "completers"
  )

ratio.df = near_completes_total_by_gender_year |>
  left_join(near_completes_total_with_stp_by_gender_year) |>
  left_join(completers_agg_by_gender_age_year) |>
  rename("gender" = "tpid_lgnd_cd")

# we want the adjusted ratio from column L (C_Outc12_13_14RatiosByGender)
# (or just the normal ratio for nc for this year)
ratio.df <- ratio.df |>
  mutate(across(where(is.numeric), ~ replace_na(., 0))) |>
  mutate(n_nc_stp = count - nc_with_early_or_late) |>
  mutate(ratio = n_nc_stp / completers)

ratio.df2 <- ratio.df |>
  filter(
    prgm_credential_awarded_name %in%
      c("Associate Degree", "University Transfer")
  ) |>
  mutate(prgm_credential_awarded_name = "Associate Degree") |>
  summarise(
    ratio_adgt = sum(n_nc_stp) / sum(completers),
    .by = c(gender, age_group, prgm_credential_awarded_name)
  )

t_dacso_near_completers_ratio_by_gender_year <-
  ratio.df |>
  left_join(ratio.df2) |>
  mutate(
    ratio = if_else(
      prgm_credential_awarded_name %in%
        c("Associate Degree", "University Transfer"),
      ratio_adgt,
      ratio
    )
  ) |>
  mutate(across(where(is.double), ~ na_if(., Inf))) |>
  mutate_all(function(x) ifelse(is.nan(x), NA, x)) |>
  select(-ratio_adgt) |>
  # subtract one here so that it's the first half of the school year
  mutate(
    year = as.numeric(paste0('20', str_sub(coci_subm_cd, 7, 8))) - 1
  )


# GOT TO HERE...leaving for now but the TTRAIN flag is available if we want to include it.

# ---- TTRAIN tables ----
# This part is not completed  - see documentation
# Note: the first query filters on cosc_grad_status_lgds_cd_group = '3'
#dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4_TTRAIN)
#dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN)
#dbExecute(decimal_con, qry99_Near_completes_program_dist_count)
#
#dbExecute(decimal_con, "DROP TABLE Near_completes_total_by_CIP4_TTRAIN")
#dbExecute(decimal_con, "DROP TABLE Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN")
#
## ---- HISTORICAL TTRAIN queries ----
## note: this uses the same intermediate table names as the above, so make sure the 2 drops are performed
#dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4_TTRAIN_history)
#dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN_history)
#dbExecute(decimal_con, qry99_Near_completes_program_dist_count_history)
#
#dbExecute(decimal_con, "DROP TABLE Near_completes_total_by_CIP4_TTRAIN")
#dbExecute(decimal_con, "DROP TABLE Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN")

# ---- Clean Up ----
# TODO: clean up this section
tables_to_keep <- c(
  "stp_enrolment",
  "stp_credential",
  "stp_enrolment_record_type",
  "stp_credential_record_type",
  "stp_enrolment_valid",
  "age_group_lookup",
  "credential_rank",
  "credential",
  "credential_non_dup",
  "credential_sup_vars",
  "tbl_credential_highest_rank",
  "tbl_credential_delay_effect",
  "outcome_credential",
  "min_enrolment",
  "t_dacso_near_completers_ratio_by_gender_year",
  "t_dacso_near_completers_ratio_by_gender",
  "t_dacso_nearcompleters_ratioageatgradcip4"
)
rm(list = setdiff(ls(), tables_to_keep))
dbDisconnect(decimal_con)
