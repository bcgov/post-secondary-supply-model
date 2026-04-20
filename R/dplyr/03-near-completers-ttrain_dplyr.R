# Near Completers TTRAIN — dplyr Translation
# Original: R/03-near-completers-ttrain.R
#
# Pipeline context:
#   Computes near-completer ratios by age group, credential, CIP4 code, gender, and
#   TTRAIN status. Near-completers (grad_status='3') who later earned an STP credential
#   are identified and subtracted. The ratios feed into program projections (step 06).
#
# Input tables:
#   - T_DACSO_Data_Part_1 — DACSO outcomes data (from 02b-1)
#   - Credential_Non_Dup — deduplicated credentials (from 02a)
#   - dbo.STP_Credential — credential records (for PSI_PEN)
#   - tmp_tbl_Age, tmp_tbl_Age_AppendNewYears — age/date data
#   - combine_creds, AgeGroupLookup, CredentialRank, stp_dacso_prgm_credential_lookup — lookups
#   - t_pssm_projection_cred_grp — credential groupings
#
# Output tables:
#   - T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN — TTRAIN ratios
#   - T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history — historical TTRAIN
#   - T_DACSO_Near_Completers_RatioAgeAtGradCIP4 — CIP4 ratios
#   - T_DACSO_Near_Completers_RatioByGender — gender ratios
#   - T_DACSO_Near_Completers_RatioByGender_year — historical gender ratios

library(tidyverse)
library(odbc)
library(DBI)
library(config)
library(glue)

my_schema <- config::get("myschema")

sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}


# ---- Connect to Decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")


# ******************************************************************************
# PART C: DERIVE AGE AT GRAD
# ******************************************************************************
# WHY: DACSO records have birth date (BTHDT) and end date (ENDDT) in YYYYMM format.
# We need to compute age at graduation for grouping near-completers and graduates.
# Original: 11 SQL operations (ALTER TABLE, UPDATE, INSERT INTO, DROP TABLE)
# Translated: dplyr string/date operations in memory.

# Pull the age data
tmp_age_append <- sch_tbl("tmp_tbl_Age_AppendNewYears") %>% collect() |> rename_with(toupper)
tmp_age <- sch_tbl("tmp_tbl_Age") %>% collect() |> rename_with(toupper)

# Clean birth/end dates: YYYYMM → MM/1/YYYY string → Date type
tmp_age_append <- tmp_age_append %>%
  mutate(
    BTHDT_CLEANED = paste0(substr(BTHDT, 5, 6), "/1/", substr(BTHDT, 1, 4)),
    ENDDT_CLEANED = case_when(
      ENDDT == "000000" ~ "",
      TRUE ~ paste0(substr(ENDDT, 5, 6), "/1/", substr(ENDDT, 1, 4))
    )
  ) %>%
  mutate(
    BTHDT_DATE = as.Date(BTHDT_CLEANED, format = "%m/%d/%Y"),
    ENDDT_DATE = as.Date(ENDDT_CLEANED, format = "%m/%d/%Y")
  ) %>%
  mutate(ENDDT_DATE = if_else(is.na(ENDDT_CLEANED) | ENDDT_CLEANED == "", as.Date(NA), ENDDT_DATE))

# Append to persistent age table
tmp_age_append <- tmp_age_append %>%
  select(COCI_STQU_ID, COCI_SUBM_CD, BTHDT_DATE, ENDDT_DATE, COCI_AGE_AT_SURVEY) %>%
  rename(TPID_DATE_OF_BIRTH = BTHDT_DATE, COSC_GRAD_CREDENTIAL_DATE = ENDDT_DATE,
         COSC_ENRL_END_DATE = ENDDT_DATE)

# Bind with existing age data
tmp_age <- bind_rows(tmp_age, tmp_age_append)

# Compute Age_At_Grad with birthday adjustment
# WHY: Simple year diff overcounts by 1 if graduation date falls before birthday
# in that calendar year.
tmp_age <- tmp_age %>%
  mutate(
    grad_date = coalesce(COSC_GRAD_CREDENTIAL_DATE, COSC_ENRL_END_DATE),
    Age_At_Grad = if_else(
      is.na(TPID_DATE_OF_BIRTH) | is.na(grad_date), NA_real_,
      as.numeric(substr(as.character(grad_date), 1, 4)) -
        as.numeric(substr(as.character(TPID_DATE_OF_BIRTH), 1, 4)) -
        if_else(
          as.numeric(substr(as.character(grad_date), 6, 7)) <
            as.numeric(substr(as.character(TPID_DATE_OF_BIRTH), 6, 7)), 1, 0
        )
    )
  )

# Pull T_DACSO_Data_Part_1 and add Age_At_Grad
t_dacso_data_part_1 <- sch_tbl("T_DACSO_Data_Part_1") %>% collect() |> rename_with(toupper)

t_dacso_data_part_1 <- t_dacso_data_part_1 %>%
  left_join(
    tmp_age %>% select(COCI_STQU_ID, Age_At_Grad),
    by = "COCI_STQU_ID"
  )

# Update the persistent tmp_tbl_Age table
dbWriteTable(con, "tmp_tbl_Age", tmp_age, overwrite = TRUE)


# ******************************************************************************
# PART D: TEMP SELECTION TABLE
# ******************************************************************************
# WHY: A column-subset version of T_DACSO_Data_Part_1 used for intermediate computations.
# Original: SELECT INTO
# Translated: select() in memory.

t_dacso_data_part_1_tempselection <- t_dacso_data_part_1 %>%
  select(COCI_STQU_ID, COCI_SUBM_CD, Age_At_Grad,
         COSC_GRAD_STATUS_LGDS_CD_GROUP, PRGM_CREDENTIAL_AWARDED,
         PRGM_CREDENTIAL_AWARDED_NAME, PSSM_CREDENTIAL, PSSM_CREDENTIAL_NAME)


# ******************************************************************************
# PART E: ADD PEN TO CREDENTIAL_NON_DUP
# ******************************************************************************
# WHY: Need PSI_PEN for PEN-based matching between DACSO and STP data.
# Original: ALTER TABLE + UPDATE...INNER JOIN
# Translated: left_join in memory, write back.

credential_non_dup <- sch_tbl("Credential_Non_Dup") %>%
  select(ID, PSI_PEN, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR,
         FINAL_CIP_CODE_4, PSI_CODE, OUTCOMES_CRED) %>%
  collect() |> rename_with(toupper)

if (!"PSI_PEN" %in% colnames(credential_non_dup) || all(is.na(credential_non_dup$PSI_PEN))) {
  stp_cred_pen <- sch_tbl("STP_Credential") %>%
    select(ID, PSI_PEN) %>%
    collect() |> rename_with(toupper)

  credential_non_dup <- credential_non_dup %>%
    select(-any_of("PSI_PEN")) %>%
    left_join(stp_cred_pen, by = "ID")

  dbWriteTable(con, "Credential_Non_Dup", credential_non_dup, overwrite = TRUE)
  rm(stp_cred_pen)
}


# ******************************************************************************
# PART F: DACSO MATCHING TO STP CREDENTIAL
# ******************************************************************************
# WHY: Match DACSO survey respondents to their STP credential records on PEN,
# then compute match flags (credential type, CIP4, CIP2, award year, institution).
# Original: 15 SQL operations (SELECT INTO, ALTER TABLE, UPDATE)
# Translated: inner_join + mutate pipeline.

# Pull lookup table for credential name mapping
stp_dacso_prgm_cred_lookup <- sch_tbl("stp_dacso_prgm_credential_lookup") %>%
  collect() |> rename_with(toupper)

# Join DACSO to Credential_Non_Dup on PEN
dacso_matching_stp_credential_pen <- t_dacso_data_part_1 %>%
  filter(COCI_PEN != "" & !is.na(COCI_PEN)) %>%
  inner_join(
    credential_non_dup %>% filter(OUTCOMES_CRED == "DACSO") %>% select(-OUTCOMES_CRED),
    by = c("COCI_PEN" = "PSI_PEN")
  )

# Add credential name from lookup
dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen %>%
  left_join(
    stp_dacso_prgm_cred_lookup %>%
      select(PRGRM_CREDENTIAL_AWARDED, PRGM_CREDENTIAL_AWARDED_NAME = PRGM_CREDENTIAL_AWARDED_NAME),
    by = c("PRGM_CREDENTIAL_AWARDED" = "PRGRM_CREDENTIAL_AWARDED")
  )

# ---- Compute match flags ----
# WHY: Each flag indicates whether a specific dimension matches between DACSO and STP.

# Award year mapping: each survey cycle maps to two valid award school years
award_year_map <- tribble(
  ~COCI_SUBM_CD, ~PSI_AWARD_SCHOOL_YEAR,
  "C_Outc07", "2004/2005", "C_Outc07", "2005/2006",
  "C_Outc08", "2005/2006", "C_Outc08", "2006/2007",
  "C_Outc09", "2006/2007", "C_Outc09", "2007/2008",
  "C_Outc10", "2007/2008", "C_Outc10", "2008/2009",
  "C_Outc11", "2008/2009", "C_Outc11", "2009/2010",
  "C_Outc12", "2009/2010", "C_Outc12", "2010/2011",
  "C_Outc13", "2010/2011", "C_Outc13", "2011/2012",
  "C_Outc14", "2011/2012", "C_Outc14", "2012/2013",
  "C_Outc15", "2012/2013", "C_Outc15", "2013/2014",
  "C_Outc16", "2013/2014", "C_Outc16", "2014/2015",
  "C_Outc17", "2014/2015", "C_Outc17", "2015/2016",
  "C_Outc18", "2015/2016", "C_Outc18", "2016/2017",
  "C_Outc19", "2016/2017", "C_Outc19", "2017/2018",
  "C_Outc20", "2017/2018", "C_Outc20", "2018/2019",
  "C_Outc21", "2018/2019", "C_Outc21", "2019/2020",
  "C_Outc22", "2019/2020", "C_Outc22", "2020/2021",
  "C_Outc23", "2020/2021", "C_Outc23", "2021/2022"
)

# Institution code aliases
inst_aliases <- tribble(
  ~PSI_CODE, ~COCI_INST_CD,
  "CAP",     "CAPU",  "CAPU",    "CAP",
  "KWAN",    "KPU",   "KPU",     "KWAN",
  "OLA",     "TRUOL", "TRUOL",   "OLA",
  "MALA",    "VIU",   "VIU",     "MALA",
  "OUC",     "OKAN",  "OKAN",    "OUC",
  "UCFV",    "UFV",   "UFV",     "UCFV",
  "UCC",     "TRU",   "TRU",     "UCC",
  "NWCC",    "CMTN",  "CMTN",    "NWCC"
)

dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen %>%
  mutate(
    match_credential = if_else(
      !is.na(PRGm_CREDENTIAL_AWARDED_NAME) &
        PRGM_CREDENTIAL_AWARDED_NAME == PSI_CREDENTIAL_CATEGORY, "yes", NA_character_
    ),
    match_cip_code_4 = if_else(
      !is.na(LCP4_CD) & !is.na(FINAL_CIP_CODE_4) & LCP4_CD == FINAL_CIP_CODE_4,
      "yes", NA_character_
    ),
    match_CIP_CODE_2 = if_else(
      !is.na(LCP4_CD) & !is.na(FINAL_CIP_CODE_4) &
        substr(LCP4_CD, 1, 2) == substr(FINAL_CIP_CODE_4, 1, 2),
      "Yes", NA_character_
    ),
    match_award_school_year = if_else(
      paste(COCI_SUBM_CD, PSI_AWARD_SCHOOL_YEAR) %in%
        paste(award_year_map$COCI_SUBM_CD, award_year_map$PSI_AWARD_SCHOOL_YEAR),
      "yes", NA_character_
    ),
    match_inst = if_else(
      PSI_CODE == COCI_INST_CD |
        paste(PSI_CODE, COCI_INST_CD) %in% paste(inst_aliases$PSI_CODE, inst_aliases$COCI_INST_CD),
      "yes", NA_character_
    )
  ) %>%
  mutate(
    match_all_4_flag = if_else(
      match_credential == "yes" & match_cip_code_4 == "yes" &
        match_award_school_year == "yes" & match_inst == "yes",
      "yes", NA_character_
    ),
    final_consider_a_match = if_else(
      match_all_4_flag == "yes", "yes",
      if_else(
        match_credential == "yes" & match_CIP_CODE_2 == "Yes" &
          match_award_school_year == "yes" & match_inst == "yes" &
          is.na(match_cip_code_4),
        "yes", NA_character_
      )
    )
  )


# ******************************************************************************
# PART G: FLAG NEAR-COMPLETERS WITH EARLIER/LATER CREDENTIAL
# ******************************************************************************
# WHY: Near-completers (grad_status='3') who earned an STP credential before or
# after their DACSO survey should be subtracted from the near-completer count.
# This section identifies those credentials and resolves duplicates.
# Original: ~56 SQL operations
# Translated: dplyr pipeline with group_by + slice_max for deduplication.

# ---- Find near-completers with STP credential matches ----
nearcompleters_step1 <- t_dacso_data_part_1 %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    COCI_SUBM_CD %in% paste0("C_Outc", c("07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"))
  ) %>%
  inner_join(
    dacso_matching_stp_credential_pen %>%
      select(COCI_STQU_ID, ID, PSI_AWARD_SCHOOL_YEAR, everything()),
    by = "COCI_STQU_ID"
  ) %>%
  filter(!is.na(ID))

# ---- Determine before/after credential timing ----
# WHY: Each survey cycle has valid "before" award years. If the STP credential
# year falls within the "before" window, it was earned before the DACSO survey.
before_year_map <- tribble(
  ~COCI_SUBM_CD, ~PSI_AWARD_SCHOOL_YEAR,
  "C_Outc07", "2002/2003", "C_Outc07", "2003/2004", "C_Outc07", "2004/2005", "C_Outc07", "2005/2006",
  "C_Outc08", "2002/2003", "C_Outc08", "2003/2004", "C_Outc08", "2004/2005", "C_Outc08", "2005/2006", "C_Outc08", "2006/2007",
  "C_Outc09", "2002/2003", "C_Outc09", "2003/2004", "C_Outc09", "2004/2005", "C_Outc09", "2005/2006", "C_Outc09", "2006/2007", "C_Outc09", "2007/2008",
  "C_Outc10", "2002/2003", "C_Outc10", "2003/2004", "C_Outc10", "2004/2005", "C_Outc10", "2005/2006", "C_Outc10", "2006/2007", "C_Outc10", "2007/2008", "C_Outc10", "2008/2009",
  "C_Outc11", "2002/2003", "C_Outc11", "2003/2004", "C_Outc11", "2004/2005", "C_Outc11", "2005/2006", "C_Outc11", "2006/2007", "C_Outc11", "2007/2008", "C_Outc11", "2008/2009", "C_Outc11", "2009/2010",
  "C_Outc12", "2002/2003", "C_Outc12", "2003/2004", "C_Outc12", "2004/2005", "C_Outc12", "2005/2006", "C_Outc12", "2006/2007", "C_Outc12", "2007/2008", "C_Outc12", "2008/2009", "C_Outc12", "2009/2010", "C_Outc12", "2010/2011",
  "C_Outc13", "2002/2003", "C_Outc13", "2003/2004", "C_Outc13", "2004/2005", "C_Outc13", "2005/2006", "C_Outc13", "2006/2007", "C_Outc13", "2007/2008", "C_Outc13", "2008/2009", "C_Outc13", "2009/2010", "C_Outc13", "2010/2011", "C_Outc13", "2011/2012",
  "C_Outc14", "2002/2003", "C_Outc14", "2003/2004", "C_Outc14", "2004/2005", "C_Outc14", "2005/2006", "C_Outc14", "2006/2007", "C_Outc14", "2007/2008", "C_Outc14", "2008/2009", "C_Outc14", "2009/2010", "C_Outc14", "2010/2011", "C_Outc14", "2011/2012", "C_Outc14", "2012/2013",
  "C_Outc15", "2002/2003", "C_Outc15", "2003/2004", "C_Outc15", "2004/2005", "C_Outc15", "2005/2006", "C_Outc15", "2006/2007", "C_Outc15", "2007/2008", "C_Outc15", "2008/2009", "C_Outc15", "2009/2010", "C_Outc15", "2010/2011", "C_Outc15", "2011/2012", "C_Outc15", "2012/2013", "C_Outc15", "2013/2014",
  "C_Outc16", "2002/2003", "C_Outc16", "2003/2004", "C_Outc16", "2004/2005", "C_Outc16", "2005/2006", "C_Outc16", "2006/2007", "C_Outc16", "2007/2008", "C_Outc16", "2008/2009", "C_Outc16", "2009/2010", "C_Outc16", "2010/2011", "C_Outc16", "2011/2012", "C_Outc16", "2012/2013", "C_Outc16", "2013/2014", "C_Outc16", "2014/2015",
  "C_Outc17", "2002/2003", "C_Outc17", "2003/2004", "C_Outc17", "2004/2005", "C_Outc17", "2005/2006", "C_Outc17", "2006/2007", "C_Outc17", "2007/2008", "C_Outc17", "2008/2009", "C_Outc17", "2009/2010", "C_Outc17", "2010/2011", "C_Outc17", "2011/2012", "C_Outc17", "2012/2013", "C_Outc17", "2013/2014", "C_Outc17", "2014/2015", "C_Outc17", "2015/2016",
  "C_Outc18", "2002/2003", "C_Outc18", "2003/2004", "C_Outc18", "2004/2005", "C_Outc18", "2005/2006", "C_Outc18", "2006/2007", "C_Outc18", "2007/2008", "C_Outc18", "2008/2009", "C_Outc18", "2009/2010", "C_Outc18", "2010/2011", "C_Outc18", "2011/2012", "C_Outc18", "2012/2013", "C_Outc18", "2013/2014", "C_Outc18", "2014/2015", "C_Outc18", "2015/2016", "C_Outc18", "2016/2017",
  "C_Outc19", "2002/2003", "C_Outc19", "2003/2004", "C_Outc19", "2004/2005", "C_Outc19", "2005/2006", "C_Outc19", "2006/2007", "C_Outc19", "2007/2008", "C_Outc19", "2008/2009", "C_Outc19", "2009/2010", "C_Outc19", "2010/2011", "C_Outc19", "2011/2012", "C_Outc19", "2012/2013", "C_Outc19", "2013/2014", "C_Outc19", "2014/2015", "C_Outc19", "2015/2016", "C_Outc19", "2016/2017", "C_Outc19", "2017/2018",
  "C_Outc20", "2002/2003", "C_Outc20", "2003/2004", "C_Outc20", "2004/2005", "C_Outc20", "2005/2006", "C_Outc20", "2006/2007", "C_Outc20", "2007/2008", "C_Outc20", "2008/2009", "C_Outc20", "2009/2010", "C_Outc20", "2010/2011", "C_Outc20", "2011/2012", "C_Outc20", "2012/2013", "C_Outc20", "2013/2014", "C_Outc20", "2014/2015", "C_Outc20", "2015/2016", "C_Outc20", "2016/2017", "C_Outc20", "2017/2018", "C_Outc20", "2018/2019",
  "C_Outc21", "2002/2003", "C_Outc21", "2003/2004", "C_Outc21", "2004/2005", "C_Outc21", "2005/2006", "C_Outc21", "2006/2007", "C_Outc21", "2007/2008", "C_Outc21", "2008/2009", "C_Outc21", "2009/2010", "C_Outc21", "2010/2011", "C_Outc21", "2011/2012", "C_Outc21", "2012/2013", "C_Outc21", "2013/2014", "C_Outc21", "2014/2015", "C_Outc21", "2015/2016", "C_Outc21", "2016/2017", "C_Outc21", "2017/2018", "C_Outc21", "2018/2019", "C_Outc21", "2019/2020",
  "C_Outc22", "2002/2003", "C_Outc22", "2003/2004", "C_Outc22", "2004/2005", "C_Outc22", "2005/2006", "C_Outc22", "2006/2007", "C_Outc22", "2007/2008", "C_Outc22", "2008/2009", "C_Outc22", "2009/2010", "C_Outc22", "2010/2011", "C_Outc22", "2011/2012", "C_Outc22", "2012/2013", "C_Outc22", "2013/2014", "C_Outc22", "2014/2015", "C_Outc22", "2015/2016", "C_Outc22", "2016/2017", "C_Outc22", "2017/2018", "C_Outc22", "2018/2019", "C_Outc22", "2019/2020", "C_Outc22", "2020/2021",
  "C_Outc23", "2002/2003", "C_Outc23", "2003/2004", "C_Outc23", "2004/2005", "C_Outc23", "2005/2006", "C_Outc23", "2006/2007", "C_Outc23", "2007/2008", "C_Outc23", "2008/2009", "C_Outc23", "2009/2010", "C_Outc23", "2010/2011", "C_Outc23", "2011/2012", "C_Outc23", "2012/2013", "C_Outc23", "2013/2014", "C_Outc23", "2014/2015", "C_Outc23", "2015/2016", "C_Outc23", "2016/2017", "C_Outc23", "2017/2018", "C_Outc23", "2018/2019", "C_Outc23", "2019/2020", "C_Outc23", "2020/2021", "C_Outc23", "2021/2022"
)

nearcompleters_step1 <- nearcompleters_step1 %>%
  mutate(
    STP_Credential_Awarded_Before_DACSO = if_else(
      paste(COCI_SUBM_CD, PSI_AWARD_SCHOOL_YEAR) %in%
        paste(before_year_map$COCI_SUBM_CD, before_year_map$PSI_AWARD_SCHOOL_YEAR),
      "Yes", NA_character_
    ),
    STP_Credential_Awarded_After_DACSO = if_else(
      is.na(STP_Credential_Awarded_Before_DACSO), "Yes", NA_character_
    )
  )

# ---- Resolve duplicates: pick max award year, then max ID for ties ----
# WHY: Near-completers matching multiple STP credentials get deduplicated by picking
# the most recent award year, then breaking ties with the max ID.
nearcompleters_step1 <- nearcompleters_step1 %>%
  group_by(COCI_STQU_ID) %>%
  mutate(n_creds = n()) %>%
  ungroup() %>%
  mutate(Has_Multiple_STP_Credentials = if_else(n_creds > 1, "Yes", NA_character_)) %>%
  group_by(COCI_STQU_ID) %>%
  arrange(desc(PSI_AWARD_SCHOOL_YEAR), desc(ID), .by_group = TRUE) %>%
  mutate(row_num = row_number()) %>%
  ungroup() %>%
  mutate(
    Final_Record_To_Use = if_else(n_creds == 1 | row_num == 1, "Yes", NA_character_),
    Dup_STQUID_UseThisRecord = if_else(n_creds > 1 & row_num == 1, "Yes", NA_character_)
  )

# ---- Create T_DACSO_NearCompleters ----
T_DACSO_NearCompleters <- t_dacso_data_part_1 %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64
  ) %>%
  select(COCI_STQU_ID, COCI_SUBM_CD, Age_At_Grad, COSC_GRAD_STATUS_LGDS_CD_GROUP,
         PRGM_CREDENTIAL_AWARDED, PRGM_CREDENTIAL_AWARDED_NAME, PSSM_CREDENTIAL, PSSM_CREDENTIAL_NAME)

# Propagate before/after flags from finalized records
final_flags <- nearcompleters_step1 %>%
  filter(Final_Record_To_Use == "Yes") %>%
  select(COCI_STQU_ID, STP_Credential_Awarded_Before_DACSO,
         STP_Credential_Awarded_After_DACSO, Has_Multiple_STP_Credentials)

T_DACSO_NearCompleters <- T_DACSO_NearCompleters %>%
  left_join(final_flags, by = "COCI_STQU_ID") %>%
  mutate(
    STP_Credential_Awarded_Before_DACSO_Final = STP_Credential_Awarded_Before_DACSO,
    STP_Credential_Awarded_After_DACSO_Final = STP_Credential_Awarded_After_DACSO
  )

# ---- Update Has_STP_Credential and Grad_Status_Factoring_in_STP ----
# WHY: Near-completers who earned an STP credential are reclassified as completers.
has_stp <- T_DACSO_NearCompleters %>%
  filter(STP_Credential_Awarded_Before_DACSO == "Yes" |
         STP_Credential_Awarded_After_DACSO == "Yes") %>%
  distinct(COCI_STQU_ID) %>%
  mutate(Has_STP_Credential = "Yes")

t_dacso_data_part_1_tempselection <- t_dacso_data_part_1_tempselection %>%
  left_join(has_stp, by = "COCI_STQU_ID") %>%
  mutate(
    Has_STP_Credential = coalesce(Has_STP_Credential, NA_character_),
    Grad_Status_Factoring_in_STP = COSC_GRAD_STATUS_LGDS_CD_GROUP,
    Grad_Status_Factoring_in_STP = if_else(
      COSC_GRAD_STATUS_LGDS_CD_GROUP == "3" & Has_STP_Credential == "Yes",
      "1", Grad_Status_Factoring_in_STP
    )
  )

# Propagate to main table
t_dacso_data_part_1 <- t_dacso_data_part_1 %>%
  left_join(
    t_dacso_data_part_1_tempselection %>%
      select(COCI_STQU_ID, Has_STP_Credential, Grad_Status_Factoring_in_STP),
    by = "COCI_STQU_ID"
  )

# Update the matching table with Dup_STQUID flags
dup_flags <- nearcompleters_step1 %>%
  filter(!is.na(Dup_STQUID_UseThisRecord)) %>%
  select(COCI_STQU_ID, ID, Dup_STQUID_UseThisRecord)

dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen %>%
  left_join(
    dup_flags %>% rename(DUP_FLAG = Dup_STQUID_UseThisRecord),
    by = c("COCI_STQU_ID", "ID")
  ) %>%
  mutate(Dup_STQUID_UseThisRecord = coalesce(DUP_FLAG, Dup_STQUID_UseThisRecord)) %>%
  select(-DUP_FLAG)

# Write updated main tables back to DB
dbWriteTable(con, "T_DACSO_Data_Part_1", t_dacso_data_part_1, overwrite = TRUE)
dbWriteTable(con, "T_DACSO_DATA_Part_1_TempSelection",
             t_dacso_data_part_1_tempselection, overwrite = TRUE)

rm(nearcompleters_step1, final_flags, has_stp, dup_flags, T_DACSO_NearCompleters)


# ******************************************************************************
# PART H: DIAGNOSTIC CHECKS (kept as comments for reference)
# ******************************************************************************
# Original: 5 PIVOT/GROUP BY diagnostic queries
# These are read-only diagnostics — translate to dplyr if needed.
# Example: t_dacso_data_part_1_tempselection %>%
#   filter(!is.na(COSC_GRAD_STATUS_LGDS_CD_GROUP), Age_At_Grad >= 17, Age_At_Grad <= 64) %>%
#   count(COSC_GRAD_STATUS_LGDS_CD_GROUP, COCI_SUBM_CD) %>%
#   pivot_wider(names_from = COCI_SUBM_CD, values_from = n, values_fill = 0)


# ******************************************************************************
# PART I: CIP4 RATIO COMPUTATIONS
# ******************************************************************************
# WHY: Compute near-completer/completer ratios by CIP4 code and age group.
# Four aggregations feed into a single ratio computation.
# Original: 17 SQL operations (SELECT INTO, ALTER TABLE, UPDATE, DROP TABLE)
# Translated: dplyr aggregation pipelines in memory.

# Pull lookup tables
combine_creds <- sch_tbl("combine_creds") %>% collect() |> rename_with(toupper)
age_group_lookup <- sch_tbl("AgeGroupLookup") %>% collect() |> rename_with(toupper)
credential_rank <- sch_tbl("CredentialRank") %>% collect() |> rename_with(toupper)

# Helper: clean lcip4_cred labels (strip "- 0" or "- 1" suffixes, fix "1 -" → "3 -")
clean_lcip4 <- function(df) {
  df %>%
    mutate(
      lcip4_cred = gsub("^1 - ", "3 - ", lcip4_cred),
      lcip4_cred = gsub(" - 0 $| - 1 $", "", lcip4_cred)
    )
}

# ---- Near-completers total by CIP4 ----
nc_cip4 <- t_dacso_data_part_1 %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64
  ) %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  group_by(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED, LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  summarise(Count = n(), .groups = "drop")

nc_cip4_combined <- nc_cip4 %>%
  inner_join(
    combine_creds %>% filter(USE_IN_PSSM_2017_18 == "Yes"),
    by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")
  ) %>%
  clean_lcip4() %>%
  group_by(AGEGROUP, LCIP4_CRED, LCP4_CD) %>%
  summarise(Count = sum(COMBINED_CRED_COUNT, na.rm = TRUE), .groups = "drop")

# ---- Near-completers with STP credential by CIP4 ----
nc_stp_cip4 <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  inner_join(
    t_dacso_data_part_1_tempselection %>%
      select(COCI_STQU_ID, Has_STP_Credential),
    by = "COCI_STQU_ID"
  ) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    Has_STP_Credential == "Yes"
  ) %>%
  group_by(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED, LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  summarise(Count = n(), .groups = "drop")

nc_stp_cip4_combined <- nc_stp_cip4 %>%
  inner_join(
    combine_creds %>% filter(USE_IN_PSSM_2017_18 == "Yes"),
    by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")
  ) %>%
  clean_lcip4() %>%
  group_by(AGEGROUP, LCIP4_CRED, LCP4_CD) %>%
  summarise(nc_with_earlier_or_later = sum(COMBINED_CRED_COUNT, na.rm = TRUE), .groups = "drop")

# ---- Completers factoring in STP by CIP4 ----
comp_stp_cip4 <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    Grad_Status_Factoring_in_STP == "1",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64
  ) %>%
  group_by(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED, LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  summarise(Count = n(), .groups = "drop")

comp_stp_cip4_combined <- comp_stp_cip4 %>%
  mutate(lcip4_cred = if_else(grepl("^1 - ", LCIP4_CRED),
                               sub("^1 - ", "3 - ", LCIP4_CRED), LCIP4_CRED)) %>%
  inner_join(
    combine_creds %>% filter(USE_IN_PSSM_2017_18 == "Yes"),
    by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")
  ) %>%
  clean_lcip4() %>%
  group_by(AGEGROUP, LCIP4_CRED, LCP4_CD) %>%
  summarise(completers = sum(COMBINED_CRED_COUNT, na.rm = TRUE), .groups = "drop")

# ---- Raw completers by CIP4 ----
comp_cip4 <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "1",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64
  ) %>%
  group_by(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED, LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  summarise(Count = n(), .groups = "drop")

comp_cip4_combined <- comp_cip4 %>%
  mutate(lcip4_cred = if_else(grepl("^1 - ", LCIP4_CRED),
                               sub("^1 - ", "3 - ", LCIP4_CRED), LCIP4_CRED)) %>%
  inner_join(
    combine_creds %>% filter(USE_IN_PSSM_2017_18 == "Yes"),
    by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")
  ) %>%
  clean_lcip4() %>%
  group_by(AGEGROUP, LCIP4_CRED, LCP4_CD) %>%
  summarise(c_not_factoring_stp = sum(COMBINED_CRED_COUNT, na.rm = TRUE), .groups = "drop")

# ---- Compute final CIP4 ratios ----
T_DACSO_Near_Completers_RatioAgeAtGradCIP4 <- nc_cip4_combined %>%
  left_join(nc_stp_cip4_combined, by = c("AGEGROUP", "LCIP4_CRED", "LCP4_CD")) %>%
  left_join(comp_stp_cip4_combined, by = c("AGEGROUP", "LCIP4_CRED", "LCP4_CD")) %>%
  left_join(comp_cip4_combined, by = c("AGEGROUP", "LCIP4_CRED", "LCP4_CD")) %>%
  replace_na(list(nc_with_earlier_or_later = 0, completers = 0, c_not_factoring_stp = 0)) %>%
  mutate(
    near_completers_stp_cred = Count - nc_with_earlier_or_later,
    ratio = if_else(completers == 0, NA_real_, near_completers_stp_cred / completers),
    ratio_not_factoring_stp = if_else(c_not_factoring_stp == 0, NA_real_,
                                       near_completers_stp_cred / c_not_factoring_stp),
    ratio = na_if(ratio, Inf),
    ratio_not_factoring_stp = na_if(ratio_not_factoring_stp, Inf)
  ) %>%
  rename(age_group = AGEGROUP, count = Count)

dbWriteTable(con, "T_DACSO_Near_Completers_RatioAgeAtGradCIP4",
             T_DACSO_Near_Completers_RatioAgeAtGradCIP4, overwrite = TRUE)


# ******************************************************************************
# PART J: GENDER RATIO COMPUTATIONS
# ******************************************************************************
# WHY: Near-completer ratios by gender (tpid_lgnd_cd), age group, and credential.
# Original: 3 SELECT INTO + dbReadTable + dplyr ratio computation
# Translated: All in-memory aggregation.

nc_gender <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    TPID_LGND_CD != "0"
  ) %>%
  count(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, TPID_LGND_CD, name = "Count")

nc_stp_gender <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  inner_join(
    t_dacso_data_part_1_tempselection %>% select(COCI_STQU_ID, Has_STP_Credential),
    by = "COCI_STQU_ID"
  ) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    Has_STP_Credential == "Yes",
    TPID_LGND_CD != "0"
  ) %>%
  count(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, TPID_LGND_CD, name = "nc_with_early_or_late")

comp_gender <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "1",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    TPID_LGND_CD != "0"
  ) %>%
  count(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, TPID_LGND_CD, name = "completers")

T_DACSO_Near_Completers_RatioByGender <- nc_gender %>%
  left_join(nc_stp_gender, by = c("AGEGROUP", "PRGM_CREDENTIAL_AWARDED_NAME", "TPID_LGND_CD")) %>%
  left_join(comp_gender, by = c("AGEGROUP", "PRGM_CREDENTIAL_AWARDED_NAME", "TPID_LGND_CD")) %>%
  replace_na(list(nc_with_early_or_late = 0, completers = 0)) %>%
  mutate(
    n_nc_stp = Count - nc_with_early_or_late,
    ratio = if_else(completers == 0, NA_real_, n_nc_stp / completers),
    ratio = na_if(ratio, Inf)
  ) %>%
  rename(gender = TPID_LGND_CD, age_group = AGEGROUP)

dbWriteTable(con, "T_DACSO_Near_Completers_RatioByGender",
             T_DACSO_Near_Completers_RatioByGender, overwrite = TRUE)


# ******************************************************************************
# PART K: GENDER RATIO BY YEAR (HISTORICAL)
# ******************************************************************************
# WHY: Same as Part J but broken out by survey year for historical analysis.
# Original: 3 SELECT INTO + dplyr ratio computation
# Translated: Same aggregation with COCI_SUBM_CD in group_by.

nc_gender_year <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    TPID_LGND_CD != "0"
  ) %>%
  count(COCI_SUBM_CD, AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, TPID_LGND_CD, name = "Count")

nc_stp_gender_year <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  inner_join(
    t_dacso_data_part_1_tempselection %>% select(COCI_STQU_ID, Has_STP_Credential),
    by = "COCI_STQU_ID"
  ) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    Has_STP_Credential == "Yes",
    TPID_LGND_CD != "0"
  ) %>%
  count(COCI_SUBM_CD, AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, TPID_LGND_CD, name = "nc_with_early_or_late")

comp_gender_year <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "1",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    TPID_LGND_CD != "0"
  ) %>%
  count(COCI_SUBM_CD, AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, TPID_LGND_CD, name = "completers")

T_DACSO_Near_Completers_RatioByGender_year <- nc_gender_year %>%
  left_join(nc_stp_gender_year,
    by = c("COCI_SUBM_CD", "AGEGROUP", "PRGM_CREDENTIAL_AWARDED_NAME", "TPID_LGND_CD")) %>%
  left_join(comp_gender_year,
    by = c("COCI_SUBM_CD", "AGEGROUP", "PRGM_CREDENTIAL_AWARDED_NAME", "TPID_LGND_CD")) %>%
  replace_na(list(nc_with_early_or_late = 0, completers = 0)) %>%
  mutate(
    n_nc_stp = Count - nc_with_early_or_late,
    ratio = if_else(completers == 0, NA_real_, n_nc_stp / completers),
    ratio = na_if(ratio, Inf),
    year = as.numeric(paste0("20", substr(COCI_SUBM_CD, nchar(COCI_SUBM_CD) - 1, nchar(COCI_SUBM_CD)))) - 1
  ) %>%
  rename(gender = TPID_LGND_CD, age_group = AGEGROUP)

dbWriteTable(con, "T_DACSO_Near_Completers_RatioByGender_year",
             T_DACSO_Near_Completers_RatioByGender_year, overwrite = TRUE)


# ******************************************************************************
# PART L: TTRAIN TABLES (CURRENT)
# ******************************************************************************
# WHY: Near-completer ratios with TTRAIN (trades training) dimension for CIP4-based
# program projections. Uses C_Outc19/20 as representative years.
# Original: 3 SELECT INTO + 2 DROP TABLE
# Translated: In-memory aggregation.

t_pssm_proj_cred_grp <- sch_tbl("t_pssm_projection_cred_grp") %>% collect() |> rename_with(toupper)

nc_ttrain <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64
  ) %>%
  group_by(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED, LCP4_CD,
            LCP4_CIP_4DIGITS_NAME, TTRAIN, COSC_GRAD_STATUS_LGDS_CD_GROUP) %>%
  summarise(Count = n(), .groups = "drop")

nc_stp_ttrain <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  inner_join(
    t_dacso_data_part_1_tempselection %>% select(COCI_STQU_ID, Has_STP_Credential),
    by = "COCI_STQU_ID"
  ) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    Has_STP_Credential == "Yes"
  ) %>%
  group_by(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED, LCP4_CD,
            LCP4_CIP_4DIGITS_NAME, TTRAIN) %>%
  summarise(stp_count = n(), .groups = "drop")

T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN <- nc_ttrain %>%
  left_join(t_pssm_proj_cred_grp %>%
              select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL),
            by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PSSM_PROJECTION_CREDENTIAL")) %>%
  left_join(nc_stp_ttrain,
    by = c("AGEGROUP", "PRGM_CREDENTIAL_AWARDED_NAME", "LCIP4_CRED", "LCP4_CD", "TTRAIN")
  ) %>%
  mutate(
    Near_completers_from_C_Outc19_20_with_earlier_or_later_STP = coalesce(stp_count, 0L),
    Near_completers_STP_Credentials = Count - Near_completers_from_C_Outc19_20_with_earlier_or_later_STP,
    PSSM_CRED = paste0(COSC_GRAD_STATUS_LGDS_CD_GROUP, " - ", PSSM_CREDENTIAL)
  ) %>%
  rename(age_group = AGEGROUP)

dbWriteTable(con, "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN",
             T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN, overwrite = TRUE)


# ******************************************************************************
# PART M: TTRAIN TABLES (HISTORICAL)
# ******************************************************************************
# WHY: Same as Part L but broken out by survey year for historical analysis.
# Original: 3 SELECT INTO + 2 DROP TABLE
# Translated: Same aggregation with COCI_SUBM_CD in group_by.

nc_ttrain_hist <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64
  ) %>%
  group_by(COCI_SUBM_CD, AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED,
            LCP4_CD, LCP4_CIP_4DIGITS_NAME, TTRAIN, COSC_GRAD_STATUS_LGDS_CD_GROUP) %>%
  summarise(Count = n(), .groups = "drop")

nc_stp_ttrain_hist <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  inner_join(
    t_dacso_data_part_1_tempselection %>% select(COCI_STQU_ID, Has_STP_Credential),
    by = "COCI_STQU_ID"
  ) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    Has_STP_Credential == "Yes"
  ) %>%
  group_by(COCI_SUBM_CD, AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED,
            LCP4_CD, LCP4_CIP_4DIGITS_NAME, TTRAIN) %>%
  summarise(stp_count = n(), .groups = "drop")

T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history <- nc_ttrain_hist %>%
  left_join(t_pssm_proj_cred_grp %>%
              select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL),
            by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PSSM_PROJECTION_CREDENTIAL")) %>%
  left_join(nc_stp_ttrain_hist,
    by = c("COCI_SUBM_CD", "AGEGROUP", "PRGM_CREDENTIAL_AWARDED_NAME",
           "LCIP4_CRED", "LCP4_CD", "TTRAIN")
  ) %>%
  mutate(
    Near_completers_from_C_Outc19_20_with_earlier_or_later_STP = coalesce(stp_count, 0L),
    Near_completers_STP_Credentials = Count - Near_completers_from_C_Outc19_20_with_earlier_or_later_STP,
    PSSM_CRED = paste0(COSC_GRAD_STATUS_LGDS_CD_GROUP, " - ", PSSM_CREDENTIAL)
  ) %>%
  rename(age_group = AGEGROUP)

dbWriteTable(con, "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history",
             T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history, overwrite = TRUE)


# ******************************************************************************
# FINAL CLEANUP
# ******************************************************************************
# Drop lookup tables no longer needed
# KEPT AS SQL: DROP TABLE (cleanup)
dbExecute(con, "DROP TABLE IF EXISTS stp_dacso_prgm_credential_lookup")
dbExecute(con, "DROP TABLE IF EXISTS tbl_Age")
dbExecute(con, "DROP TABLE IF EXISTS AgeGroupLookup")
dbExecute(con, "DROP TABLE IF EXISTS combine_creds")
dbExecute(con, "DROP TABLE IF EXISTS t_pssm_projection_cred_grp")

# Write the matching table back (needed by downstream scripts)
dbWriteTable(con, "DACSO_Matching_STP_Credential_PEN",
             dacso_matching_stp_credential_pen, overwrite = TRUE)

dbDisconnect(con)
