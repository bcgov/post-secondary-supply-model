# Aligns CIP codes between BGS survey and STP data
# Required Tables
#   INFOWARE_BGS_DIST_19_23
#   INFOWARE_BGS_DIST_18_22
#   INFOWARE_BGS_COHORT_INFO
#   INFOWARE_L_CIP_6DIGITS_CIP2016
#   INFOWARE_L_CIP_4DIGITS_CIP2016
#   INFOWARE_L_CIP_2DIGITS_CIP2016
#   Credential_non_Dup
#   STP_Credential  - for PSI_PEN
# Resulting Tables
#   T_BGS_Data_Final_for_OutcomesMatching
#   Credential_Non_Dup_BGS_IDs
#   Credential_Non_Dup_GRAD_IDs

options(java.parameters = " -Xmx102400m") ## For reading oracle tables

library(tidyverse)
library(odbc)
library(DBI)
library(glue)
library(RJDBC)
library(dbplyr)
library(config)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

# Connect to Decimal
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Read in INFOWARE tables ----
# Note: These tables should be loaded by 'R/load-infoware-lookups.R'
# We check for their existence and proceed.

required_tables <- c(
  "INFOWARE_BGS_DIST_19_23",
  "INFOWARE_BGS_DIST_18_22",
  "INFOWARE_BGS_COHORT_INFO",
  "INFOWARE_L_CIP_6DIGITS_CIP2016",
  "INFOWARE_L_CIP_4DIGITS_CIP2016",
  "INFOWARE_L_CIP_2DIGITS_CIP2016"
)

missing_tables <- required_tables[!map_lgl(required_tables, ~dbExistsTable(con, Id(schema = my_schema, table = .x)))]

if (length(missing_tables) > 0) {
  stop(glue::glue("The following required tables are missing in schema '{my_schema}': {paste(missing_tables, collapse = ', ')}. Please run 'R/load-infoware-lookups.R' first."))
}

# ---- Table References ----
infoware_bgs_19_23 <- tbl(con, in_schema(my_schema, "INFOWARE_BGS_DIST_19_23"))
infoware_bgs_18_22 <- tbl(con, in_schema(my_schema, "INFOWARE_BGS_DIST_18_22"))
infoware_cohort_info <- tbl(con, in_schema(my_schema, "INFOWARE_BGS_COHORT_INFO"))

cip_6_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_6DIGITS_CIP2016"))
cip_4_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_4DIGITS_CIP2016"))
cip_2_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_2DIGITS_CIP2016"))

credential_non_dup_tbl <- tbl(con, in_schema(my_schema, "credential_non_dup"))
stp_credential_tbl <- tbl(con, in_schema(my_schema, "STP_Credential"))


# ---- Part 1: Build Outcomes Data ----
# Created tables: T_BGS_Data_Final_for_OutcomesMatching

# Step 1: 2020 Outcomes (from 19_23 table)
t_bgs_step1 <- infoware_bgs_19_23 %>%
  inner_join(infoware_cohort_info, by = "STQU_ID") %>%
  select(
    PEN, STUDID, STQU_ID, SRV_Y_N, RESPONDENT, Year, SUBM_CD, 
    INSTITUTION_CODE, INSTITUTION, CIP2DIG, CIP2DIG_NAME, CIP4DIG, 
    CIP_4DIGIT_NO_PERIOD, CIP4DIG_NAME, CIP_6DIGIT_1, CIP_6DIGIT_NO_PERIOD, 
    CIP6DIG_NAME, PROGRAM, DASHBOARD_PROGRAM, CPC
  )

# Step 2: 2018 Outcomes (from 18_22 table, filtered for Year 2018)
t_bgs_step2 <- infoware_bgs_18_22 %>%
  filter(Year == 2018) %>%
  inner_join(infoware_cohort_info, by = "STQU_ID") %>%
  select(
    PEN, STUDID, STQU_ID, SRV_Y_N, RESPONDENT, Year, SUBM_CD, 
    INSTITUTION_CODE, INSTITUTION, CIP2DIG, CIP2DIG_NAME, CIP4DIG, 
    CIP_4DIGIT_NO_PERIOD, CIP4DIG_NAME, CIP_6DIGIT_1, CIP_6DIGIT_NO_PERIOD, 
    CIP6DIG_NAME, PROGRAM, DASHBOARD_PROGRAM, CPC
  )

# Combine and Add PSSM_CREDENTIAL
t_bgs_final <- union_all(t_bgs_step1, t_bgs_step2) %>%
  mutate(PSSM_CREDENTIAL = "BACH") %>%
  compute(name = "T_BGS_Data_Final_for_OutcomesMatching", temporary = FALSE, schema = my_schema, overwrite = TRUE)


# ---- Part 2: Create Credential (STP) 4D and 2D CIP Codes ----
# Created tables: Credential_Non_Dup_BGS_IDs, Credential_Non_Dup_GRAD_IDs

# 1. Create cleaning table (collect STP BGS/GRAD data)
stp_cip_cleaning <- credential_non_dup_tbl %>%
  filter(OUTCOMES_CRED %in% c("BGS", "GRAD")) %>%
  group_by(PSI_CREDENTIAL_CIP, OUTCOMES_CRED) %>%
  summarize(Expr1 = n(), .groups = "drop") %>%
  mutate(PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP) %>%
  # Cleaning logic: add trailing/leading zeros
  mutate(
    PSI_CREDENTIAL_CIP = case_when(
      nchar(PSI_CREDENTIAL_CIP) == 6 & !str_detect(substr(PSI_CREDENTIAL_CIP, 1, 2), "\\.") ~ paste0(PSI_CREDENTIAL_CIP, "0"),
      nchar(PSI_CREDENTIAL_CIP) == 6 ~ paste0("0", PSI_CREDENTIAL_CIP),
      TRUE ~ PSI_CREDENTIAL_CIP
    )
  ) %>%
  # Helper columns for joins
  mutate(
    CIP_5 = substr(PSI_CREDENTIAL_CIP, 1, 5),
    CIP_2 = substr(PSI_CREDENTIAL_CIP, 1, 2)
  )

# 2. Add 4 and 2D CIP codes from INFOWARE
# Match on full CIP
stp_cip_cleaning <- stp_cip_cleaning %>%
  left_join(
    cip_6_tbl %>% select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD),
    by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
  ) %>%
  rename(STP_CIP_CODE_4 = LCIP_LCP4_CD, STP_CIP_CODE_2 = LCIP_LCP2_CD)

# Match on first 5 chars
stp_cip_cleaning <- stp_cip_cleaning %>%
  left_join(
    cip_6_tbl %>%
      mutate(CIP_5_lookup = substr(LCIP_CD_WITH_PERIOD, 1, 5)) %>%
      select(CIP_5_lookup, LCP4_alt = LCIP_LCP4_CD, LCP2_alt = LCIP_LCP2_CD) %>%
      distinct(),
    by = c("CIP_5" = "CIP_5_lookup")
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, LCP4_alt),
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCP2_alt)
  ) %>%
  select(-LCP4_alt, -LCP2_alt)

# Recode general programs
general_cips <- c("11.00", "13.00", "14.00", "19.00", "23.00", "24.00", "26.00", "40.00", "42.00", "45.00", "50.00", "52.00", "55.00")
stp_cip_cleaning <- stp_cip_cleaning %>%
  mutate(
    STP_CIP_CODE_4 = case_when(
      is.na(STP_CIP_CODE_4) & CIP_5 %in% general_cips ~ paste0(CIP_2, "01"),
      TRUE ~ STP_CIP_CODE_4
    )
  )

# Match on first 2 digits for 2D code
stp_cip_cleaning <- stp_cip_cleaning %>%
  left_join(
    cip_6_tbl %>%
      mutate(CIP_2_lookup = substr(LCIP_CD_WITH_PERIOD, 1, 2)) %>%
      select(CIP_2_lookup, LCP2_alt2 = LCIP_LCP2_CD) %>%
      distinct(),
    by = c("CIP_2" = "CIP_2_lookup")
  ) %>%
  mutate(
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCP2_alt2)
  ) %>%
  select(-LCP2_alt2)

# 3. Add names
stp_cip_cleaning <- stp_cip_cleaning %>%
  left_join(cip_4_tbl %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME), by = c("STP_CIP_CODE_4" = "LCP4_CD")) %>%
  left_join(cip_2_tbl %>% select(LCP2_CD, LCP2_DIGITS_NAME), by = c("STP_CIP_CODE_2" = "LCP2_CD")) %>%
  rename(STP_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME, STP_CIP_CODE_2_NAME = LCP2_DIGITS_NAME) %>%
  mutate(STP_CIP_CODE_4_NAME = coalesce(STP_CIP_CODE_4_NAME, "Invalid 4-digit CIP")) %>%
  compute(name = "Credential_Non_Dup_STP_CIP4_Cleaning", temporary = FALSE, schema = my_schema, overwrite = TRUE)

# 4. Create BGS and GRAD ID Tables
# Note: GRAD table logic is simpler (just sets FINAL = STP), BGS will have matching logic later

# Common join for both
stp_cip_ids <- credential_non_dup_tbl %>%
  filter(OUTCOMES_CRED %in% c("BGS", "GRAD")) %>%
  inner_join(
    stp_cip_cleaning,
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_orig", "OUTCOMES_CRED")
  )

# BGS IDs (Needs PSI_PEN from STP_Credential if missing)
bgs_ids_base <- stp_cip_ids %>%
  filter(OUTCOMES_CRED == "BGS") %>%
  select(
    ID, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, 
    PSI_CREDENTIAL_CIP, PSI_AWARD_SCHOOL_YEAR, OUTCOMES_CRED,
    STP_CIP_CODE_4, STP_CIP_CODE_4_NAME, STP_CIP_CODE_2, STP_CIP_CODE_2_NAME
  ) %>%
  # Handle Unspecified
  mutate(PSI_PROGRAM_CODE = if_else(PSI_PROGRAM_CODE == "(Unspecified)", NA_character_, PSI_PROGRAM_CODE))

# Add PSI_PEN logic (Join to STP_Credential)
credential_bgs_ids <- bgs_ids_base %>%
  left_join(stp_credential_tbl %>% select(ID, PSI_PEN), by = "ID") %>%
  compute(name = "Credential_Non_Dup_BGS_IDs", temporary = FALSE, schema = my_schema, overwrite = TRUE)

# GRAD IDs (Finalize CIPs immediately)
credential_grad_ids <- stp_cip_ids %>%
  filter(OUTCOMES_CRED == "GRAD") %>%
  select(
    ID, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP, PSI_AWARD_SCHOOL_YEAR, OUTCOMES_CRED,
    FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME
  ) %>%
  compute(name = "Credential_Non_Dup_GRAD_IDs", temporary = FALSE, schema = my_schema, overwrite = TRUE)


# ---- Part 3: Build Case-level XWALK ----
# Created table: BGS_Matching_STP_Credential_PEN

# 1. Match BGS and STP on PEN
bgs_matching <- t_bgs_final %>%
  filter(PEN != "", !is.na(PEN), PEN != "0") %>%
  inner_join(
    credential_bgs_ids, 
    by = c("PEN" = "PSI_PEN")
  ) %>%
  select(
    STQU_ID, ID, PEN, OUTCOMES_CRED,
    INSTITUTION_CODE, PSI_CODE,
    YEAR, PSI_AWARD_SCHOOL_YEAR,
    BGS_FINAL_CIP_CODE_4 = CIP_4DIGIT_NO_PERIOD,
    BGS_FINAL_CIP_CODE_4_NAME = CIP4DIG_NAME,
    STP_FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    STP_FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    BGS_FINAL_CIP_CODE_2 = CIP2DIG,
    BGS_FINAL_CIP_CODE_2_NAME = CIP2DIG_NAME,
    STP_FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    STP_FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME,
    BGS_PROGRAM_CODE = CPC,
    BGS_PROGRAM_DESC = PROGRAM,
    STP_PROGRAM_CODE = PSI_PROGRAM_CODE,
    STP_PROGRAM_DESC = PSI_CREDENTIAL_PROGRAM_DESCRIPTION
  )

# 2. Add Match Flags
# Define Institution Match Logic (case_when)
bgs_matching_flagged <- bgs_matching %>%
  mutate(
    Match_Inst = case_when(
      PSI_CODE == INSTITUTION_CODE ~ "Yes",
      PSI_CODE == 'CAPU' & INSTITUTION_CODE == 'CAP' ~ "Yes",
      PSI_CODE == 'CAP' & INSTITUTION_CODE == 'CAPU' ~ "Yes",
      PSI_CODE == 'DOUG' & INSTITUTION_CODE == 'DGL' ~ "Yes",
      PSI_CODE == 'UCC' & INSTITUTION_CODE == 'TRU' ~ "Yes",
      PSI_CODE %in% c('ECIAD', 'ECU') & INSTITUTION_CODE %in% c('ECU', 'ECUAD', 'ECIAD') ~ "Yes",
      PSI_CODE %in% c('KWAN', 'KPU') & INSTITUTION_CODE %in% c('KPU', 'KWN') ~ "Yes",
      PSI_CODE %in% c('MALA', 'MAL') & INSTITUTION_CODE %in% c('VIU', 'MAL') ~ "Yes",
      PSI_CODE %in% c('OUC', 'OKAN') & INSTITUTION_CODE %in% c('OKAN', 'OKN', 'OUC') ~ "Yes",
      PSI_CODE == 'OLA' & INSTITUTION_CODE == 'TRUOL' ~ "Yes",
      PSI_CODE %in% c('UCFV', 'UFV') & INSTITUTION_CODE %in% c('UFV', 'FVAL', 'UCFV') ~ "Yes",
      PSI_CODE == 'UBCO' & INSTITUTION_CODE == 'UBC' ~ "Yes",
      PSI_CODE == 'UBCV' & INSTITUTION_CODE == 'UBC' ~ "Yes",
      TRUE ~ NA_character_
    ),
    Match_Award_School_Year = case_when(
      (YEAR == 2000 & PSI_AWARD_SCHOOL_YEAR %in% c('1997/1998', '1998/1999')) |
      (YEAR == 2002 & PSI_AWARD_SCHOOL_YEAR %in% c('1999/2000', '2000/2001')) |
      (YEAR == 2004 & PSI_AWARD_SCHOOL_YEAR %in% c('2001/2002', '2002/2003')) |
      (YEAR == 2006 & PSI_AWARD_SCHOOL_YEAR %in% c('2003/2004', '2004/2005')) |
      (YEAR == 2008 & PSI_AWARD_SCHOOL_YEAR %in% c('2005/2006', '2006/2007')) |
      (YEAR == 2009 & PSI_AWARD_SCHOOL_YEAR %in% c('2006/2007', '2007/2008')) |
      (YEAR == 2010 & PSI_AWARD_SCHOOL_YEAR %in% c('2007/2008', '2008/2009')) |
      (YEAR == 2011 & PSI_AWARD_SCHOOL_YEAR %in% c('2008/2009', '2009/2010')) |
      (YEAR == 2012 & PSI_AWARD_SCHOOL_YEAR %in% c('2009/2010', '2010/2011')) |
      (YEAR == 2013 & PSI_AWARD_SCHOOL_YEAR %in% c('2010/2011', '2011/2012')) |
      (YEAR == 2014 & PSI_AWARD_SCHOOL_YEAR %in% c('2011/2012', '2012/2013')) |
      (YEAR == 2015 & PSI_AWARD_SCHOOL_YEAR %in% c('2012/2013', '2013/2014')) |
      (YEAR == 2016 & PSI_AWARD_SCHOOL_YEAR %in% c('2013/2014', '2014/2015')) |
      (YEAR == 2017 & PSI_AWARD_SCHOOL_YEAR %in% c('2014/2015', '2015/2016')) |
      (YEAR == 2018 & PSI_AWARD_SCHOOL_YEAR %in% c('2015/2016', '2016/2017')) |
      (YEAR == 2019 & PSI_AWARD_SCHOOL_YEAR %in% c('2016/2017', '2017/2018')) |
      (YEAR == 2020 & PSI_AWARD_SCHOOL_YEAR %in% c('2017/2018', '2018/2019')) |
      (YEAR == 2021 & PSI_AWARD_SCHOOL_YEAR %in% c('2018/2019', '2019/2020')) |
      (YEAR == 2022 & PSI_AWARD_SCHOOL_YEAR %in% c('2019/2020', '2020/2021')) |
      (YEAR == 2023 & PSI_AWARD_SCHOOL_YEAR %in% c('2020/2021', '2021/2022')) ~ "Yes",
      TRUE ~ NA_character_
    ),
    Match_CIP_CODE_4 = if_else(BGS_FINAL_CIP_CODE_4 == STP_FINAL_CIP_CODE_4, "Yes", NA_character_),
    Match_CIP_CODE_2 = if_else(BGS_FINAL_CIP_CODE_2 == STP_FINAL_CIP_CODE_2, "Yes", NA_character_)
  ) %>%
  mutate(
    Match_All_3_CIP4_Flag = if_else(Match_CIP_CODE_4 == "Yes" & Match_Award_School_Year == "Yes" & Match_Inst == "Yes", "Yes", NA_character_),
    Match_All_3_CIP2_Flag = if_else(Match_CIP_CODE_2 == "Yes" & Match_Award_School_Year == "Yes" & Match_Inst == "Yes", "Yes", NA_character_)
  ) %>%
  # Initialize Final Columns
  mutate(
    FINAL_CIP_CODE_4 = if_else(Match_All_3_CIP4_Flag == "Yes", BGS_FINAL_CIP_CODE_4, NA_character_),
    FINAL_CIP_CODE_2 = if_else(Match_All_3_CIP4_Flag == "Yes", BGS_FINAL_CIP_CODE_2, NA_character_),
    USE_BGS_CIP = if_else(Match_All_3_CIP4_Flag == "Yes", "Yes", NA_character_),
    Final_Consider_A_Match = if_else(Match_All_3_CIP4_Flag == "Yes", "Yes", NA_character_)
  )

# 3. Save Intermediate for Manual Matching Support
bgs_matching_flagged %>%
  compute(name = "BGS_Matching_STP_Credential_PEN", temporary = FALSE, schema = my_schema, overwrite = TRUE)


# ---- Part 3C: Manual matching ----
# Prepare data for manual review (Simulating the CSV export/import workflow)
bgs_matching_tbl <- tbl(con, in_schema(my_schema, "BGS_Matching_STP_Credential_PEN"))

manual_candidates <- bgs_matching_tbl %>%
  filter(Match_Inst == "Yes", Match_Award_School_Year == "Yes", is.na(Final_Consider_A_Match)) %>%
  collect()

# Note: In a real interactive session, you would write 'manual_candidates' to CSV, edit, and read back.
# Here we simulate the result of that manual process or use a placeholder logic.
# Since we cannot pause for user input, we will implement the auto-logic for the "Check Match Inst Award Year Only"
# which effectively defaults to STP if no manual override is provided.

# Logic: Default to STP if not decided
manual_updates <- manual_candidates %>%
  mutate(
    USE_BGS_CIP = coalesce(USE_BGS_CIP, "No"), # Default to No (Use STP)
    FINAL_CIP_CODE_4 = if_else(USE_BGS_CIP == "Yes", BGS_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_2 = if_else(USE_BGS_CIP == "Yes", BGS_FINAL_CIP_CODE_2, STP_FINAL_CIP_CODE_2),
    Final_Probable_Match = "Yes"
  ) %>%
  select(STQU_ID, ID, FINAL_CIP_CODE_4, FINAL_CIP_CODE_2, USE_BGS_CIP, Final_Probable_Match)

# Upload updates to DB and Update Main Table
if (nrow(manual_updates) > 0) {
  copy_to(con, manual_updates, "tmp_manual_updates", overwrite = TRUE)
  
  bgs_matching_updated <- bgs_matching_tbl %>%
    left_join(
      tbl(con, "tmp_manual_updates"), 
      by = c("STQU_ID", "ID")
    ) %>%
    mutate(
      FINAL_CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4.y, FINAL_CIP_CODE_4.x),
      FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2.y, FINAL_CIP_CODE_2.x),
      USE_BGS_CIP = coalesce(USE_BGS_CIP.y, USE_BGS_CIP.x),
      Final_Probable_Match = coalesce(Final_Probable_Match.y, Final_Probable_Match.x)
    ) %>%
    select(-ends_with(".y"), -ends_with(".x"))
} else {
  bgs_matching_updated <- bgs_matching_tbl
}


# Part 3D: Final Fill (CIP Names and Clusters)
bgs_matching_final <- bgs_matching_updated %>%
  # Default remaining to STP
  mutate(
    FINAL_CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, STP_FINAL_CIP_CODE_2),
    USE_BGS_CIP = coalesce(USE_BGS_CIP, "No")
  ) %>%
  # Add Names
  left_join(cip_4_tbl %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME), by = c("FINAL_CIP_CODE_4" = "LCP4_CD")) %>%
  left_join(cip_2_tbl %>% select(LCP2_CD, LCP2_DIGITS_NAME, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME), by = c("FINAL_CIP_CODE_2" = "LCP2_CD")) %>%
  rename(
    FINAL_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME,
    FINAL_CIP_CODE_2_NAME = LCP2_DIGITS_NAME,
    FINAL_CIP_CLUSTER_CODE = LCP2_LCIPPC_CD,
    FINAL_CIP_CLUSTER_NAME = LCP2_LCIPPC_NAME
  ) %>%
  # Default remaining cluster info
  left_join(cip_2_tbl %>% select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME), by = c("FINAL_CIP_CODE_2" = "LCP2_CD")) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME) %>%
  compute(name = "BGS_Matching_STP_Credential_PEN", temporary = FALSE, schema = my_schema, overwrite = TRUE)


# ---- Part 4: Update Credential_Non_Dup_BGS_IDs ----
# Created table: Credential_Non_Dup_BGS_IDs (Updated)

credential_bgs_updated <- tbl(con, in_schema(my_schema, "Credential_Non_Dup_BGS_IDs")) %>%
  left_join(
    bgs_matching_final %>%
      filter(!is.na(Final_Consider_A_Match) | !is.na(Final_Probable_Match)) %>%
      select(ID, 
             MATCH_FINAL_CIP_4 = FINAL_CIP_CODE_4, 
             MATCH_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
             MATCH_FINAL_CIP_2 = FINAL_CIP_CODE_2,
             MATCH_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME,
             MATCH_FINAL_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
             MATCH_FINAL_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
             MATCH_USE_BGS = USE_BGS_CIP,
             MATCH_BGS_CIP_4 = BGS_FINAL_CIP_CODE_4,
             MATCH_BGS_CIP_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
             Final_Consider_A_Match, Final_Probable_Match),
    by = "ID"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = coalesce(MATCH_FINAL_CIP_4, STP_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = coalesce(MATCH_FINAL_CIP_4_NAME, STP_CIP_CODE_4_NAME),
    FINAL_CIP_CODE_2 = coalesce(MATCH_FINAL_CIP_2, STP_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(MATCH_FINAL_CIP_2_NAME, STP_CIP_CODE_2_NAME),
    FINAL_CIP_CLUSTER_CODE = MATCH_FINAL_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME = MATCH_FINAL_CLUSTER_NAME,
    USE_BGS_CIP = coalesce(MATCH_USE_BGS, "No because no match"),
    OUTCOMES_CIP_CODE_4 = MATCH_BGS_CIP_4,
    OUTCOMES_CIP_CODE_4_NAME = MATCH_BGS_CIP_4_NAME
  ) %>%
  # Fill missing cluster info for unmatched
  left_join(cip_2_tbl %>% select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME), by = c("FINAL_CIP_CODE_2" = "LCP2_CD")) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME) %>%
  # Default remaining to STP
  mutate(
    FINAL_CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, STP_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, STP_CIP_CODE_4_NAME),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, STP_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, STP_CIP_CODE_2_NAME),
    USE_BGS_CIP = coalesce(USE_BGS_CIP, "No")
  ) %>%
  compute(name = "Credential_Non_Dup_BGS_IDs", temporary = FALSE, schema = my_schema, overwrite = TRUE)


# ---- Part 5: Update T_BGS_DATA_FINAL ----
# Created table: T_BGS_Data_Final_for_OutcomesMatching (Updated)

t_bgs_updated <- tbl(con, in_schema(my_schema, "T_BGS_Data_Final_for_OutcomesMatching")) %>%
  left_join(
    bgs_matching_final %>%
      filter(!is.na(Final_Consider_A_Match) | !is.na(Final_Probable_Match)) %>%
      select(STQU_ID, 
             MATCH_FINAL_CIP_4 = FINAL_CIP_CODE_4, 
             MATCH_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
             MATCH_FINAL_CIP_2 = FINAL_CIP_CODE_2,
             MATCH_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME,
             MATCH_FINAL_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
             MATCH_FINAL_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
             MATCH_USE_BGS = USE_BGS_CIP,
             MATCH_STP_CIP_4 = STP_FINAL_CIP_CODE_4,
             MATCH_STP_CIP_4_NAME = STP_FINAL_CIP_CODE_4_NAME,
             Final_Consider_A_Match, Final_Probable_Match),
    by = "STQU_ID"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = coalesce(MATCH_FINAL_CIP_4, CIP_4DIGIT_NO_PERIOD),
    FINAL_CIP_CODE_4_NAME = coalesce(MATCH_FINAL_CIP_4_NAME, CIP4DIG_NAME),
    FINAL_CIP_CODE_2 = coalesce(MATCH_FINAL_CIP_2, CIP2DIG),
    FINAL_CIP_CODE_2_NAME = coalesce(MATCH_FINAL_CIP_2_NAME, CIP2DIG_NAME),
    FINAL_CIP_CLUSTER_CODE = MATCH_FINAL_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME = MATCH_FINAL_CLUSTER_NAME,
    USE_STP_CIP = if_else(MATCH_USE_BGS == "Yes", "No", "Yes"), # Invert logic
    STP_CIP_CODE_4 = MATCH_STP_CIP_4,
    STP_CIP_CODE_4_NAME = MATCH_STP_CIP_4_NAME
  ) %>%
  mutate(USE_STP_CIP = coalesce(USE_STP_CIP, "No because no match")) %>%
  # Fill missing cluster
  left_join(cip_2_tbl %>% select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME), by = c("FINAL_CIP_CODE_2" = "LCP2_CD")) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME) %>%
  # Default remaining to STP
  mutate(
    FINAL_CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, STP_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, STP_CIP_CODE_4_NAME),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, STP_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, STP_CIP_CODE_2_NAME),
    USE_STP_CIP = coalesce(USE_STP_CIP, "No")
  ) %>%
  compute(name = "T_BGS_Data_Final_for_OutcomesMatching", temporary = FALSE, schema = my_schema, overwrite = TRUE)

# ---- Clean up ----
dbExecute(con, "DROP TABLE tmp_manual_updates")
dbDisconnect(con)