# BGS Program Matching — dplyr Translation
# Original: R/02a-bgs-program-matching.R
#
# Pipeline context:
#   Aligns CIP codes between BGS (BC Graduate Survey) data from INFOWARE and
#   STP (Student Transitions Project) credential data. The matching is done at
#   the case level using encrypted PENs, then CIP codes are validated/corrected
#   using a multi-step matching process:
#     1. Build outcomes data from BGS survey distributions
#     2. Clean STP credential CIP codes against INFOWARE taxonomy
#     3. Build case-level crosswalk (XWALK) matching BGS to STP on PEN
#     4. Auto-match on institution, award year, and CIP codes
#     5. Manual matching for remaining uncertain cases
#     6. Update Credential_Non_Dup_BGS_IDs with final CIPs
#     7. Update T_BGS_Data_Final_for_OutcomesMatching with final CIPs
#
# Input tables:
#   - INFOWARE_BGS_DIST_19_23, INFOWARE_BGS_DIST_18_22 — BGS distributions (Oracle)
#   - INFOWARE_BGS_COHORT_INFO — BGS cohort info (Oracle)
#   - INFOWARE_L_CIP_*DIGITS_CIP2016 — CIP taxonomy (Oracle)
#   - Credential_Non_Dup — deduplicated credentials (from 02a-update-cred-non-dup)
#   - STP_Credential — credential records (for PSI_PEN)
#
# Output tables:
#   - T_BGS_Data_Final_for_OutcomesMatching — BGS data with matched CIPs
#   - Credential_Non_Dup_BGS_IDs — BGS credentials with final CIPs
#   - Credential_Non_Dup_GRAD_IDs — GRAD credentials with final CIPs
#   - BGS_Matching_STP_Credential_PEN — crosswalk table

options(java.parameters = " -Xmx102400m")

library(tidyverse)
library(RODBC)
library(odbc)
library(DBI)
library(glue)
library(RJDBC)

# Helper: reference a table in the user's schema
my_schema <- config::get("myschema")

sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}


# ******************************************************************************
# Read INFOWARE tables from Oracle
# ******************************************************************************
# WHY: BGS survey data and CIP taxonomy live in Oracle/INFOWARE. These are large
# tables (80K+ rows) that need to be transferred to SQL Server for processing.
# The chunked writes are necessary because the ODBC driver has row limits.

iw_config <- config::get("infoware")
jdbc_config <- config::get("jdbc")

jdbcDriver <- JDBC(jdbc_config$class, classPath = jdbc_config$path)

iw_con <- dbConnect(jdbcDriver,
                    iw_config$database,
                    iw_config$uid,
                    iw_config$pwd)

# !! UPDATE THE TABLES to include the desired year ranges !!
INFOWARE_BGS_DIST_19_23  <- dbReadTable(iw_con, "INFOWARE.BGS_DIST_19_23")
INFOWARE_BGS_DIST_18_22  <- dbReadTable(iw_con, "INFOWARE.BGS_DIST_18_22")
INFOWARE_BGS_COHORT_INFO <- dbReadTable(iw_con, "INFOWARE.BGS_COHORT_INFO")
INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_6DIGITS_CIP2016")
INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_4DIGITS_CIP2016")
INFOWARE_L_CIP_2DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_2DIGITS_CIP2016")

dbDisconnect(iw_con)

# ---- Connect to Decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Write INFOWARE tables to Decimal (chunked for large tables) ----
# !! UPDATE ROW NUMBERS to match actual data !!
dbWriteTable(con, "INFOWARE_BGS_DIST_19_23", INFOWARE_BGS_DIST_19_23[1:80000,])
dbWriteTable(con, "INFOWARE_BGS_DIST_19_23", INFOWARE_BGS_DIST_19_23[80001:121074,], append = TRUE)
dbWriteTable(con, "INFOWARE_BGS_DIST_18_22", INFOWARE_BGS_DIST_18_22[1:80000,])
dbWriteTable(con, "INFOWARE_BGS_DIST_18_22", INFOWARE_BGS_DIST_18_22[80001:118632,], append = TRUE)
dbWriteTable(con, "INFOWARE_BGS_COHORT_INFO", INFOWARE_BGS_COHORT_INFO[1:80000,])
dbWriteTable(con, "INFOWARE_BGS_COHORT_INFO", INFOWARE_BGS_COHORT_INFO[80001:160000,], append = TRUE)
dbWriteTable(con, "INFOWARE_BGS_COHORT_INFO", INFOWARE_BGS_COHORT_INFO[160001:240000,], append = TRUE)
dbWriteTable(con, "INFOWARE_BGS_COHORT_INFO", INFOWARE_BGS_COHORT_INFO[240001:290758,], append = TRUE)
dbWriteTable(con, "INFOWARE_L_CIP_6DIGITS_CIP2016", INFOWARE_L_CIP_6DIGITS_CIP2016)
dbWriteTable(con, "INFOWARE_L_CIP_4DIGITS_CIP2016", INFOWARE_L_CIP_4DIGITS_CIP2016)
dbWriteTable(con, "INFOWARE_L_CIP_2DIGITS_CIP2016", INFOWARE_L_CIP_2DIGITS_CIP2016)

# Remove large Oracle tables from R memory
rm(INFOWARE_BGS_DIST_19_23, INFOWARE_BGS_DIST_18_22, INFOWARE_BGS_COHORT_INFO,
   INFOWARE_L_CIP_6DIGITS_CIP2016, INFOWARE_L_CIP_4DIGITS_CIP2016,
   INFOWARE_L_CIP_2DIGITS_CIP2016)


# ******************************************************************************
# PART 1: BUILD OUTCOMES DATA
# ******************************************************************************
# WHY: Combine multiple years of BGS survey distribution data with cohort info
# to create the working outcomes matching table. The BGS data comes in year-range
# tables (e.g., 19_23, 18_22) that need to be unioned and joined with cohort
# information on STQU_ID (the student questionnaire ID).
#
# Original: Two SELECT INTO + one INSERT INTO + one ALTER TABLE + DEFAULT
# Translated: Two inner_join + bind_rows + mutate

bgs_dist_19_23 <- sch_tbl("INFOWARE_BGS_DIST_19_23") %>%
  collect() |> rename_with(toupper)

bgs_dist_18_22 <- sch_tbl("INFOWARE_BGS_DIST_18_22") %>%
  collect() |> rename_with(toupper)

bgs_cohort_info <- sch_tbl("INFOWARE_BGS_COHORT_INFO") %>%
  collect() |> rename_with(toupper)

# Step 1: Join 19_23 distribution with cohort info on STQU_ID
# The cohort info provides CIP codes, program descriptions, and CPC codes.
bgs_outcomes_19_23 <- bgs_dist_19_23 %>%
  inner_join(
    bgs_cohort_info %>%
      select(STQU_ID, PEN, STUDID, SRV_Y_N, SUBM_CD,
             CIP2DIG, CIP2DIG_NAME, CIP4DIG, CIP_4DIGIT_NO_PERIOD, CIP4DIG_NAME,
             CIP_6DIGIT_1, CIP_6DIGIT_NO_PERIOD, CIP6DIG_NAME,
             PROGRAM, DASHBOARD_PROGRAM, CPC),
    by = "STQU_ID"
  )

# Step 2: Join 18_22 distribution with cohort info, keeping only 2018 data
# WHY only 2018: Older data overlaps with the 19_23 range, so we only take the
# earliest year that doesn't overlap.
bgs_outcomes_18_22 <- bgs_dist_18_22 %>%
  filter(YEAR == 2018) %>%
  inner_join(
    bgs_cohort_info %>%
      select(STQU_ID, PEN, STUDID, SRV_Y_N, SUBM_CD,
             CIP2DIG, CIP2DIG_NAME, CIP4DIG, CIP_4DIGIT_NO_PERIOD, CIP4DIG_NAME,
             CIP_6DIGIT_1, CIP_6DIGIT_NO_PERIOD, CIP6DIG_NAME,
             PROGRAM, DASHBOARD_PROGRAM, CPC),
    by = "STQU_ID"
  )

# Combine both year ranges and add PSSM_CREDENTIAL = 'BACH' (all BGS are bachelors)
T_BGS_Data_Final_for_OutcomesMatching <- bind_rows(bgs_outcomes_19_23, bgs_outcomes_18_22) %>%
  mutate(PSSM_CREDENTIAL = "BACH")

dbWriteTable(con, "T_BGS_Data_Final_for_OutcomesMatching",
             T_BGS_Data_Final_for_OutcomesMatching, overwrite = TRUE)

rm(bgs_outcomes_19_23, bgs_outcomes_18_22, bgs_dist_19_23, bgs_dist_18_22, bgs_cohort_info)


# ******************************************************************************
# PART 2: CLEAN CREDENTIAL CIP
# ******************************************************************************
# WHY: STP credential data has CIP codes in various formats (XX.XXXX, X.XXXX,
# XX.XXX) that need to be standardized and matched against the official INFOWARE
# CIP taxonomy to produce clean 4-digit and 2-digit CIP codes. These cleaned
# codes are used for matching BGS survey data to STP credentials.
#
# Original: ~12 SQL operations (SELECT INTO, ALTER TABLE, UPDATE)
# Translated: Single dplyr pipeline with sequential CIP matching steps

# Pull source tables
credential_non_dup <- sch_tbl("Credential_Non_Dup") %>%
  select(ID, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         PSI_CREDENTIAL_CIP, PSI_AWARD_SCHOOL_YEAR, OUTCOMES_CRED) %>%
  collect() |> rename_with(toupper)

cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
  collect() |> rename_with(toupper)

cip4_ref <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2016") %>%
  collect() |> rename_with(toupper)

cip2_ref <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2016") %>%
  collect() |> rename_with(toupper)

# Build distinct match tables from the 6-digit CIP taxonomy
# WHY: CIP matching is hierarchical — try full 6-digit first, then 4-digit
# prefix, then 2-digit prefix. Each step catches codes the previous missed.
cip6_full <- cip6 %>%
  select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD) %>%
  distinct()

cip6_prefix4 <- cip6 %>%
  mutate(CIP_PREFIX_5 = substr(LCIP_CD_WITH_PERIOD, 1, 5)) %>%
  distinct(CIP_PREFIX_5, LCIP_LCP4_CD, LCIP_LCP2_CD)

cip6_prefix2 <- cip6 %>%
  mutate(CIP_PREFIX_2 = substr(LCIP_CD_WITH_PERIOD, 1, 2)) %>%
  distinct(CIP_PREFIX_2, LCIP_LCP2_CD)

# General CIP codes (00 ending) that should be recoded to 01 ending
general_cip_prefixes <- c("11.00", "13.00", "14.00", "19.00", "23.00", "24.00",
                           "26.00", "40.00", "42.00", "45.00", "50.00", "52.00",
                           "55.00")

# Build CIP cleaning table: count credentials by CIP and outcome type
cip_cleaning <- credential_non_dup %>%
  filter(OUTCOMES_CRED %in% c("BGS", "GRAD")) %>%
  count(PSI_CREDENTIAL_CIP, OUTCOMES_CRED, name = "EXPR1") %>%
  # Preserve original CIP before any cleaning
  mutate(PSI_CREDENTIAL_CIP_ORIG = PSI_CREDENTIAL_CIP) %>%
  # Fix format: XX.XXX (6 chars, no period in first 2) → XX.XXXX0
  mutate(PSI_CREDENTIAL_CIP = if_else(
    nchar(PSI_CREDENTIAL_CIP) == 6 & !grepl("\\.", substr(PSI_CREDENTIAL_CIP, 1, 2)),
    paste0(PSI_CREDENTIAL_CIP, "0"),
    PSI_CREDENTIAL_CIP
  )) %>%
  # Fix format: X.XXXX (6 chars remaining) → 0X.XXXX
  mutate(PSI_CREDENTIAL_CIP = if_else(
    nchar(PSI_CREDENTIAL_CIP) == 6,
    paste0("0", PSI_CREDENTIAL_CIP),
    PSI_CREDENTIAL_CIP
  ))

# Match CIP codes in a sequential cascade: full match → prefix-4 → general → prefix-2
cip_cleaning <- cip_cleaning %>%
  # Step 1a: Full 6-digit match to INFOWARE
  left_join(cip6_full,
    by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD"),
    suffix = c("", "_full")
  ) %>%
  rename(STP_CIP_CODE_4 = LCIP_LCP4_CD, STP_CIP_CODE_2 = LCIP_LCP2_CD) %>%
  # Step 1b: 4-digit prefix match for remaining NULLs
  mutate(CIP_PREFIX_5 = substr(PSI_CREDENTIAL_CIP, 1, 5)) %>%
  left_join(
    cip6_prefix4 %>% rename(STP_CIP_CODE_4_P4 = LCIP_LCP4_CD, STP_CIP_CODE_2_P4 = LCIP_LCP2_CD),
    by = "CIP_PREFIX_5"
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, STP_CIP_CODE_4_P4),
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, STP_CIP_CODE_2_P4)
  ) %>%
  select(-STP_CIP_CODE_4_P4, -STP_CIP_CODE_2_P4, -CIP_PREFIX_5) %>%
  # Step 1c: Recode general programs (00 ending → 01 ending)
  mutate(
    STP_CIP_CODE_4 = if_else(
      is.na(STP_CIP_CODE_4) & substr(PSI_CREDENTIAL_CIP, 1, 5) %in% general_cip_prefixes,
      paste0(substr(PSI_CREDENTIAL_CIP, 1, 2), "01"),
      STP_CIP_CODE_4
    )
  ) %>%
  # Step 1d: 2-digit prefix match for remaining NULL CIP2
  mutate(CIP_PREFIX_2 = substr(PSI_CREDENTIAL_CIP, 1, 2)) %>%
  left_join(
    cip6_prefix2 %>% rename(STP_CIP_CODE_2_P2 = LCIP_LCP2_CD),
    by = "CIP_PREFIX_2"
  ) %>%
  mutate(STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, STP_CIP_CODE_2_P2)) %>%
  select(-STP_CIP_CODE_2_P2, -CIP_PREFIX_2) %>%
  # Step 2: Add 4-digit CIP names from INFOWARE
  left_join(
    cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("STP_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  rename(STP_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME) %>%
  # Step 3: Add 2-digit CIP names from INFOWARE
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME),
    by = c("STP_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  rename(STP_CIP_CODE_2_NAME = LCP2_DIGITS_NAME) %>%
  # Step 4: Mark remaining NULL 4D names as invalid
  mutate(STP_CIP_CODE_4_NAME = if_else(
    is.na(STP_CIP_CODE_4_NAME), "Invalid 4-digit CIP", STP_CIP_CODE_4_NAME
  ))

dbWriteTable(con, "Credential_Non_Dup_STP_CIP4_Cleaning", cip_cleaning, overwrite = TRUE)

# ---- Split into BGS and GRAD credential tables ----
# WHY: BGS credentials need further CIP cleaning via XWALK matching, while GRAD
# credentials use their STP CIPs directly as final (no outcomes matching needed).
# Both are subsets of Credential_Non_Dup joined with the cleaned CIP codes.

Credential_Non_Dup_BGS_IDs <- credential_non_dup %>%
  filter(OUTCOMES_CRED == "BGS") %>%
  inner_join(
    cip_cleaning %>% select(PSI_CREDENTIAL_CIP_ORIG, OUTCOMES_CRED,
                            STP_CIP_CODE_4, STP_CIP_CODE_4_NAME,
                            STP_CIP_CODE_2, STP_CIP_CODE_2_NAME),
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_ORIG",
           "OUTCOMES_CRED" = "OUTCOMES_CRED")
  )

Credential_Non_Dup_GRAD_IDs <- credential_non_dup %>%
  filter(OUTCOMES_CRED == "GRAD") %>%
  inner_join(
    cip_cleaning %>% select(PSI_CREDENTIAL_CIP_ORIG, OUTCOMES_CRED,
                            STP_CIP_CODE_4, STP_CIP_CODE_4_NAME,
                            STP_CIP_CODE_2, STP_CIP_CODE_2_NAME),
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_ORIG",
           "OUTCOMES_CRED" = "OUTCOMES_CRED")
  ) %>%
  rename(
    FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME
  )

# Replace (Unspecified) with NA in BGS_IDs
Credential_Non_Dup_BGS_IDs <- Credential_Non_Dup_BGS_IDs %>%
  mutate(PSI_PROGRAM_CODE = if_else(PSI_PROGRAM_CODE == "(Unspecified)", NA_character_, PSI_PROGRAM_CODE))

dbWriteTable(con, "Credential_Non_Dup_BGS_IDs", Credential_Non_Dup_BGS_IDs, overwrite = TRUE)
dbWriteTable(con, "Credential_Non_Dup_GRAD_IDs", Credential_Non_Dup_GRAD_IDs, overwrite = TRUE)

rm(credential_non_dup, cip_cleaning, cip6_full, cip6_prefix4, cip6_prefix2)


# ******************************************************************************
# PART 3: BUILD CASE-LEVEL XWALK
# ******************************************************************************
# WHY: The crosswalk (XWALK) table matches individual BGS survey respondents to
# their corresponding STP credential records using encrypted PENs. This enables
# CIP code validation — when BGS and STP agree on a CIP code, we have high
# confidence in the assignment. When they disagree, analyst review is needed.

### Part 3A: Initial XWALK ----

# Add PSI_PEN from STP_Credential if not already present
# WHY: Credential_Non_Dup may not have the PEN column; it comes from STP_Credential.
if (!"PSI_PEN" %in% colnames(Credential_Non_Dup_BGS_IDs)) {
  stp_credential_pen <- sch_tbl("STP_Credential") %>%
    select(ID, PSI_PEN) %>%
    collect() |> rename_with(toupper)

  Credential_Non_Dup_BGS_IDs <- Credential_Non_Dup_BGS_IDs %>%
    left_join(stp_credential_pen, by = "ID")

  dbWriteTable(con, "Credential_Non_Dup_BGS_IDs", Credential_Non_Dup_BGS_IDs, overwrite = TRUE)
  rm(stp_credential_pen)
}

# Join BGS data with STP credentials on PEN to create the XWALK
# WHY: Matching on encrypted PEN links BGS survey respondents to their STP
# credential records. The HAVING clause in the original SQL filters out blank/zero PENs.
# In dplyr, we filter before joining to avoid creating invalid matches.
BGS_Matching_STP_Credential_PEN <- T_BGS_Data_Final_for_OutcomesMatching %>%
  filter(!is.na(PEN) & PEN != "" & PEN != "0") %>%
  inner_join(
    Credential_Non_Dup_BGS_IDs %>%
      filter(OUTCOMES_CRED == "BGS") %>%
      select(ID, PSI_PEN, OUTCOMES_CRED, PSI_CODE, PSI_AWARD_SCHOOL_YEAR,
             STP_CIP_CODE_4, STP_CIP_CODE_4_NAME, STP_CIP_CODE_2, STP_CIP_CODE_2_NAME,
             PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION),
    by = c("PEN" = "PSI_PEN")
  ) %>%
  select(
    STQU_ID, ID, PEN, OUTCOMES_CRED,
    INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR,
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
  ) %>%
  distinct()

dbWriteTable(con, "BGS_Matching_STP_Credential_PEN", BGS_Matching_STP_Credential_PEN, overwrite = TRUE)


### Part 3B: Auto matching using flags ----

# WHY: We compute match flags to classify the quality of each BGS-STP match.
# The flags indicate whether institution, award year, 4-digit CIP, and 2-digit CIP
# all agree. "All 3" matches (inst + year + CIP) are auto-accepted.

# Institution code mapping: many BC post-secondary institutions changed names/codes
# over time. This mapping handles known equivalences.
inst_code_pairs <- tribble(
  ~PSI_CODE, ~INSTITUTION_CODE,
  "CAPU",    "CAP",
  "CAP",     "CAPU",
  "DOUG",    "DGL",
  "UCC",     "TRU",
  "ECIAD",   "ECU",
  "ECIAD",   "ECUAD",
  "ECU",     "ECUAD",
  "ECU",     "ECIAD",
  "KWAN",    "KPU",
  "KWAN",    "KWN",
  "KPU",     "KWN",
  "MALA",    "VIU",
  "MALA",    "MAL",
  "OUC",     "OKAN",
  "OUC",     "OKN",
  "OKAN",    "OKN",
  "OKAN",    "OUC",
  "OLA",     "TRUOL",
  "UCFV",    "UFV",
  "UCFV",    "FVAL",
  "UFV",     "FVAL",
  "UFV",     "UCFV",
  "MAL",     "VIU",
  "UBCO",    "UBC",
  "UBCV",    "UBC"
)

# Award year mapping: BGS surveys are 2 years after graduation, and school years
# span two calendar years. This maps each BGS survey year to the two possible
# STP award school years.
award_year_map <- tribble(
  ~YEAR, ~PSI_AWARD_SCHOOL_YEAR,
  2000L, "1997/1998", 2000L, "1998/1999",
  2002L, "1999/2000", 2002L, "2000/2001",
  2004L, "2001/2002", 2004L, "2002/2003",
  2006L, "2003/2004", 2006L, "2004/2005",
  2008L, "2005/2006", 2008L, "2006/2007",
  2009L, "2006/2007", 2009L, "2007/2008",
  2010L, "2007/2008", 2010L, "2008/2009",
  2011L, "2008/2009", 2011L, "2009/2010",
  2012L, "2009/2010", 2012L, "2010/2011",
  2013L, "2010/2011", 2013L, "2011/2012",
  2014L, "2011/2012", 2014L, "2012/2013",
  2015L, "2012/2013", 2015L, "2013/2014",
  2016L, "2013/2014", 2016L, "2014/2015",
  2017L, "2014/2015", 2017L, "2015/2016",
  2018L, "2015/2016", 2018L, "2016/2017",
  2019L, "2016/2017", 2019L, "2017/2018",
  2020L, "2017/2018", 2020L, "2018/2019",
  2021L, "2018/2019", 2021L, "2019/2020",
  2022L, "2019/2020", 2022L, "2020/2021",
  2023L, "2020/2021", 2023L, "2021/2022"
)

# Compute all match flags in a single pipeline
# WHY: The original had 7 separate UPDATE statements. In dplyr, we compute all
# flags at once using vectorized operations.
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  mutate(
    # Match_Inst: same institution or known equivalent
    Match_Inst = if_else(
      PSI_CODE == INSTITUTION_CODE |
        paste(PSI_CODE, INSTITUTION_CODE) %in% paste(inst_code_pairs$PSI_CODE, inst_code_pairs$INSTITUTION_CODE),
      "Yes", NA_character_
    ),
    # Match_Award_School_Year: BGS year maps to STP award year (2-year lag)
    Match_Award_School_Year = if_else(
      paste(YEAR, PSI_AWARD_SCHOOL_YEAR) %in% paste(award_year_map$YEAR, award_year_map$PSI_AWARD_SCHOOL_YEAR),
      "Yes", NA_character_
    ),
    # Match_CIP_CODE_4: 4-digit CIP codes agree
    Match_CIP_CODE_4 = if_else(BGS_FINAL_CIP_CODE_4 == STP_FINAL_CIP_CODE_4, "Yes", NA_character_),
    # Match_CIP_CODE_2: 2-digit CIP codes agree
    Match_CIP_CODE_2 = if_else(BGS_FINAL_CIP_CODE_2 == STP_FINAL_CIP_CODE_2, "Yes", NA_character_),
    # Match_All_3_CIP4_Flag: inst + year + CIP4 all match
    Match_All_3_CIP4_Flag = if_else(
      Match_CIP_CODE_4 == "Yes" & Match_Award_School_Year == "Yes" & Match_Inst == "Yes",
      "Yes", NA_character_
    ),
    # Match_All_3_CIP2_Flag: inst + year + CIP2 all match
    Match_All_3_CIP2_Flag = if_else(
      Match_CIP_CODE_2 == "Yes" & Match_Award_School_Year == "Yes" & Match_Inst == "Yes",
      "Yes", NA_character_
    )
  )

# Initialize final columns (these get filled by the matching steps below)
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  mutate(
    Final_Consider_A_Match = NA_character_,
    Final_Probable_Match = NA_character_,
    FINAL_CIP_CODE_4 = NA_character_,
    FINAL_CIP_CODE_4_NAME = NA_character_,
    FINAL_CIP_CODE_2 = NA_character_,
    FINAL_CIP_CODE_2_NAME = NA_character_,
    FINAL_CIP_CLUSTER_CODE = NA_character_,
    FINAL_CIP_CLUSTER_NAME = NA_character_,
    USE_BGS_CIP = NA_character_
  )

# ---- Set final CIPs for Match_All_3_CIP4_Flag matches ----
# WHY: When all three match criteria agree (inst, year, CIP4), BGS and STP CIPs
# are identical, so we use BGS CIP as final (same as STP in this case).
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  mutate(
    Final_Consider_A_Match = if_else(Match_All_3_CIP4_Flag == "Yes", "Yes", Final_Consider_A_Match),
    FINAL_CIP_CODE_4 = if_else(Match_All_3_CIP4_Flag == "Yes", BGS_FINAL_CIP_CODE_4, FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_2 = if_else(Match_All_3_CIP4_Flag == "Yes", BGS_FINAL_CIP_CODE_2, FINAL_CIP_CODE_2),
    USE_BGS_CIP = if_else(Match_All_3_CIP4_Flag == "Yes", "Yes", USE_BGS_CIP)
  )

# ---- CIP2 review and matching ----
# WHY: When CIP4 doesn't match but CIP2 does (with inst + year), we need to decide
# which source has the more appropriate CIP. The logic prefers the more specific
# CIP code and falls back to STP for consistency.

# Get aggregated CIP2-only matches (CIP2 match but no CIP4 match)
matched_2d_cips <- BGS_Matching_STP_Credential_PEN %>%
  filter(Match_All_3_CIP2_Flag == "Yes" & is.na(Match_All_3_CIP4_Flag)) %>%
  group_by(INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR,
           BGS_PROGRAM_CODE, STP_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_DESC,
           BGS_FINAL_CIP_CODE_4, BGS_FINAL_CIP_CODE_4_NAME,
           STP_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4_NAME,
           BGS_FINAL_CIP_CODE_2, BGS_FINAL_CIP_CODE_2_NAME,
           STP_FINAL_CIP_CODE_2, STP_FINAL_CIP_CODE_2_NAME,
           Match_All_3_CIP2_Flag) %>%
  summarise(Expr1 = n(), .groups = "drop")

# Get full crosswalk for reference
t1 <- BGS_Matching_STP_Credential_PEN %>%
  group_by(across(everything())) %>%
  summarise(Expr1 = n(), .groups = "drop")

# Step 1: Use more detailed CIP when one source has a "general program" code
general_cip4s <- c("1101", "1301", "1401", "1901", "2301", "2401", "2601",
                    "4001", "4201", "4501", "5001", "5201", "5501")

matched_2d_cips <- matched_2d_cips %>%
  mutate(CIP_TO_USE = case_when(
    BGS_FINAL_CIP_CODE_4 %in% general_cip4s ~ "STP",
    STP_FINAL_CIP_CODE_4 %in% general_cip4s ~ "BGS",
    TRUE ~ NA_character_
  ))

# Step 2A: Match STP program to STP programs where CIPs also match — use STP
matched_2d_cips <- matched_2d_cips %>%
  left_join(
    t1 %>% distinct(INSTITUTION_CODE, STP_PROGRAM_CODE, STP_PROGRAM_DESC,
                    CIP = BGS_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4),
    by = c("INSTITUTION_CODE", "STP_PROGRAM_CODE", "STP_PROGRAM_DESC", "STP_FINAL_CIP_CODE_4")
  ) %>%
  mutate(CIP_TO_USE = if_else(!is.na(CIP_TO_USE), CIP_TO_USE,
                               if_else(!is.na(CIP), "STP", NA_character_))) %>%
  select(-CIP)

# Step 2B: Match BGS program to BGS programs where CIPs also match — use BGS
matched_2d_cips <- matched_2d_cips %>%
  left_join(
    t1 %>% distinct(INSTITUTION_CODE, BGS_PROGRAM_CODE, BGS_PROGRAM_DESC,
                    BGS_FINAL_CIP_CODE_4, CIP = STP_FINAL_CIP_CODE_4),
    by = c("INSTITUTION_CODE", "BGS_PROGRAM_CODE", "BGS_PROGRAM_DESC", "BGS_FINAL_CIP_CODE_4")
  ) %>%
  mutate(CIP_TO_USE = if_else(!is.na(CIP_TO_USE), CIP_TO_USE,
                               if_else(!is.na(CIP), "BGS", NA_character_))) %>%
  select(-CIP)

# Step 3: Custom year-specific overrides
# BGS cip=2701 (Mathematics) vs STP cip=2703 (Applied Mathematics) → use STP
matched_2d_cips <- matched_2d_cips %>%
  mutate(CIP_TO_USE = if_else(
    !is.na(CIP_TO_USE), CIP_TO_USE,
    if_else(BGS_FINAL_CIP_CODE_4 == "2701" & STP_FINAL_CIP_CODE_4 == "2703", "STP", NA_character_)
  ))

# BGS cip=1405 (Bioengineering) vs STP cip=1407 (Chemical Engineering) → use STP
matched_2d_cips <- matched_2d_cips %>%
  mutate(CIP_TO_USE = if_else(
    !is.na(CIP_TO_USE), CIP_TO_USE,
    if_else(BGS_FINAL_CIP_CODE_4 == "1405" & STP_FINAL_CIP_CODE_4 == "1407", "STP", NA_character_)
  ))

# Remaining unmatched: likely double majors with different ordering — use STP
matched_2d_cips <- matched_2d_cips %>%
  mutate(CIP_TO_USE = if_else(is.na(CIP_TO_USE), "STP", CIP_TO_USE))

# Apply CIP2 review results to the XWALK
# WHY: The review determined which source to use for each CIP2 match. We update
# the FINAL_CIP columns and USE_BGS_CIP flag accordingly.
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  left_join(
    matched_2d_cips %>%
      select(INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR,
             BGS_PROGRAM_CODE, STP_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_DESC,
             BGS_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4, Match_All_3_CIP2_Flag, CIP_TO_USE),
    by = c("INSTITUTION_CODE", "PSI_CODE", "YEAR", "PSI_AWARD_SCHOOL_YEAR",
           "BGS_PROGRAM_CODE", "STP_PROGRAM_CODE", "BGS_PROGRAM_DESC", "STP_PROGRAM_DESC",
           "BGS_FINAL_CIP_CODE_4", "STP_FINAL_CIP_CODE_4", "Match_All_3_CIP2_Flag")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = case_when(
      !is.na(FINAL_CIP_CODE_4) ~ FINAL_CIP_CODE_4,
      CIP_TO_USE == "BGS" ~ BGS_FINAL_CIP_CODE_4,
      CIP_TO_USE == "STP" ~ STP_FINAL_CIP_CODE_4,
      TRUE ~ FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_2 = case_when(
      !is.na(FINAL_CIP_CODE_2) ~ FINAL_CIP_CODE_2,
      CIP_TO_USE == "BGS" ~ BGS_FINAL_CIP_CODE_2,
      CIP_TO_USE == "STP" ~ STP_FINAL_CIP_CODE_2,
      TRUE ~ FINAL_CIP_CODE_2
    ),
    USE_BGS_CIP = case_when(
      !is.na(USE_BGS_CIP) ~ USE_BGS_CIP,
      CIP_TO_USE == "BGS" ~ "Yes",
      CIP_TO_USE == "STP" ~ "No",
      TRUE ~ USE_BGS_CIP
    ),
    Final_Consider_A_Match = if_else(!is.na(USE_BGS_CIP) & is.na(Final_Consider_A_Match),
                                      "Yes", Final_Consider_A_Match)
  ) %>%
  select(-CIP_TO_USE)

rm(matched_2d_cips, t1)


### Part 3C: Manual matching ----

# WHY: Records that match on institution and award year but not on CIP4 need
# manual review. The analyst exports unmatched program combinations to CSV,
# reviews them (choosing BGS or STP CIP), and reads the results back.

# Get records matching on inst + year but not yet assigned a final CIP
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig <- BGS_Matching_STP_Credential_PEN %>%
  filter(Match_Inst == "Yes" & Match_Award_School_Year == "Yes" & is.na(Final_Consider_A_Match)) %>%
  select(STQU_ID, ID, PEN, INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR,
         Match_Inst, Match_Award_School_Year, Match_All_3_CIP4_Flag, Match_All_3_CIP2_Flag,
         Final_Consider_A_Match, BGS_FINAL_CIP_CODE_4, BGS_FINAL_CIP_CODE_4_NAME,
         STP_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4_NAME,
         BGS_FINAL_CIP_CODE_2, BGS_FINAL_CIP_CODE_2_NAME,
         STP_FINAL_CIP_CODE_2, STP_FINAL_CIP_CODE_2_NAME,
         BGS_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_CODE, STP_PROGRAM_DESC,
         USE_BGS_CIP)

# Export for manual review
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>%
  mutate(across(everything(), trimws)) %>%
  group_by(INSTITUTION_CODE, PSI_CODE, BGS_FINAL_CIP_CODE_4, BGS_FINAL_CIP_CODE_4_NAME,
           STP_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4_NAME, BGS_PROGRAM_CODE,
           BGS_PROGRAM_DESC, STP_PROGRAM_CODE, STP_PROGRAM_DESC, USE_BGS_CIP) %>%
  summarize(Count = n(), .groups = "drop") %>%
  write_csv("BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos_orig.csv")

# ---- MANUAL STEP ----
# Analyst reviews the CSV, fills in USE_BGS_CIP column (Yes/No/x), and saves as
# BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos.csv

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos <-
  read_csv("BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos.csv")

# Join manual review back to row-level data
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>%
  mutate(across(everything(), trimws)) %>%
  select(-USE_BGS_CIP) %>%
  left_join(
    BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos %>%
      select(-BGS_FINAL_CIP_CODE_4_NAME, -STP_FINAL_CIP_CODE_4_NAME),
    by = c("INSTITUTION_CODE", "PSI_CODE", "BGS_FINAL_CIP_CODE_4",
           "STP_FINAL_CIP_CODE_4", "BGS_PROGRAM_CODE", "BGS_PROGRAM_DESC",
           "STP_PROGRAM_CODE", "STP_PROGRAM_DESC")
  )

# Set final CIPs based on manual review
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
  mutate(
    FINAL_CIP_CODE_4 = case_when(
      USE_BGS_CIP == "No" ~ STP_FINAL_CIP_CODE_4,
      USE_BGS_CIP == "Yes" ~ BGS_FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_2 = case_when(
      USE_BGS_CIP == "No" ~ STP_FINAL_CIP_CODE_2,
      USE_BGS_CIP == "Yes" ~ BGS_FINAL_CIP_CODE_2
    )
  )

# Update XWALK with manually matched CIPs
# WHY: The manual review results need to be applied to the main XWALK table.
# We match on both ID (STP credential) and STQU_ID (BGS survey) for precision.
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  left_join(
    BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
      select(ID, STQU_ID, FINAL_CIP_CODE_4_manual = FINAL_CIP_CODE_4,
             FINAL_CIP_CODE_2_manual = FINAL_CIP_CODE_2, USE_BGS_CIP_manual = USE_BGS_CIP),
    by = c("ID", "STQU_ID")
  ) %>%
  mutate(
    Final_Probable_Match = if_else(
      is.na(Final_Probable_Match) & is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_2) &
        !is.na(FINAL_CIP_CODE_4_manual),
      "Yes", Final_Probable_Match
    ),
    FINAL_CIP_CODE_4 = if_else(is.na(FINAL_CIP_CODE_4) & !is.na(FINAL_CIP_CODE_4_manual),
                                FINAL_CIP_CODE_4_manual, FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_2 = if_else(is.na(FINAL_CIP_CODE_2) & !is.na(FINAL_CIP_CODE_2_manual),
                                FINAL_CIP_CODE_2_manual, FINAL_CIP_CODE_2),
    USE_BGS_CIP = if_else(is.na(USE_BGS_CIP) & !is.na(USE_BGS_CIP_manual),
                           USE_BGS_CIP_manual, USE_BGS_CIP)
  ) %>%
  select(-FINAL_CIP_CODE_4_manual, -FINAL_CIP_CODE_2_manual, -USE_BGS_CIP_manual)

# Fill remaining unmatched records with STP CIPs as final
# WHY: Records that couldn't be matched to BGS use STP CIPs as the default.
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_4_NAME) &
        is.na(FINAL_CIP_CODE_2) & is.na(FINAL_CIP_CODE_2_NAME),
      STP_FINAL_CIP_CODE_4, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2_NAME),
      STP_FINAL_CIP_CODE_4_NAME, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      is.na(FINAL_CIP_CODE_2) & is.na(FINAL_CIP_CODE_2_NAME),
      STP_FINAL_CIP_CODE_2, FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      is.na(FINAL_CIP_CODE_2_NAME),
      STP_FINAL_CIP_CODE_2_NAME, FINAL_CIP_CODE_2_NAME
    ),
    USE_BGS_CIP = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_4_NAME) &
        is.na(FINAL_CIP_CODE_2) & is.na(FINAL_CIP_CODE_2_NAME),
      "No", USE_BGS_CIP
    )
  )

rm(BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly,
   BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig,
   BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos)


### Part 3D: Fill in Final Columns ----

# WHY: Fill CIP names and cluster codes from INFOWARE taxonomy. These were left
# NULL during the matching process and need to be populated for downstream use.
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  # Fill CIP4 names
  left_join(
    cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("FINAL_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  mutate(FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME)) %>%
  select(-LCP4_CIP_4DIGITS_NAME) %>%
  # Fill CIP2 names and cluster codes
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_DIGITS_NAME, -LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

dbWriteTable(con, "BGS_Matching_STP_Credential_PEN", BGS_Matching_STP_Credential_PEN, overwrite = TRUE)


# ******************************************************************************
# PART 4: UPDATE CREDENTIAL_NON_DUP
# ******************************************************************************
# WHY: Transfer the matched CIPs from the XWALK back to the credential tables.
# This updates Credential_Non_Dup_BGS_IDs with the final CIP assignments from
# the matching process, then handles unmatched programs.

### Part 4A: Update with XWALK ----

# Join BGS_IDs with XWALK to get final CIPs
# WHY: Step 1 fills from XWALK where Final_Consider_A_Match is set (auto-matched).
# Step 2 fills remaining from XWALK where Final_Probable_Match is set (manually matched).
xwalk_step1 <- BGS_Matching_STP_Credential_PEN %>%
  filter(!is.na(Final_Consider_A_Match) & Final_Consider_A_Match != "")

xwalk_step2 <- BGS_Matching_STP_Credential_PEN %>%
  filter(!is.na(Final_Probable_Match) & Final_Probable_Match != "")

Credential_Non_Dup_BGS_IDs <- Credential_Non_Dup_BGS_IDs %>%
  # Add empty columns for final CIP data
  mutate(
    OUTCOMES_CIP_CODE_4 = NA_character_,
    OUTCOMES_CIP_CODE_4_NAME = NA_character_,
    Final_Consider_A_Match = NA_character_,
    Final_Probable_Match = NA_character_,
    USE_BGS_CIP = NA_character_,
    FINAL_CIP_CODE_4 = NA_character_,
    FINAL_CIP_CODE_4_NAME = NA_character_,
    FINAL_CIP_CODE_2 = NA_character_,
    FINAL_CIP_CODE_2_NAME = NA_character_,
    FINAL_CIP_CLUSTER_CODE = NA_character_,
    FINAL_CIP_CLUSTER_NAME = NA_character_
  ) %>%
  # Step 1: Fill from auto-matched XWALK records
  left_join(
    xwalk_step1 %>%
      select(ID, XW_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
             XW_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
             XW_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
             XW_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
             XW_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
             XW_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
             XW_USE_BGS_CIP = USE_BGS_CIP,
             XW_OUTCOMES_CIP_CODE_4 = BGS_FINAL_CIP_CODE_4,
             XW_OUTCOMES_CIP_CODE_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
             XW_Final_Consider_A_Match = Final_Consider_A_Match,
             XW_Final_Probable_Match = Final_Probable_Match),
    by = "ID"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, XW_FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, XW_FINAL_CIP_CODE_4_NAME),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, XW_FINAL_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, XW_FINAL_CIP_CODE_2_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, XW_FINAL_CIP_CLUSTER_CODE),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, XW_FINAL_CIP_CLUSTER_NAME),
    USE_BGS_CIP = coalesce(USE_BGS_CIP, XW_USE_BGS_CIP),
    OUTCOMES_CIP_CODE_4 = coalesce(OUTCOMES_CIP_CODE_4, XW_OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = coalesce(OUTCOMES_CIP_CODE_4_NAME, XW_OUTCOMES_CIP_CODE_4_NAME),
    Final_Consider_A_Match = coalesce(Final_Consider_A_Match, XW_Final_Consider_A_Match),
    Final_Probable_Match = coalesce(Final_Probable_Match, XW_Final_Probable_Match)
  ) %>%
  select(-starts_with("XW_")) %>%
  # Step 2: Fill remaining from manually matched XWALK records
  left_join(
    xwalk_step2 %>%
      select(ID, XW_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
             XW_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
             XW_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
             XW_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
             XW_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
             XW_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
             XW_USE_BGS_CIP = USE_BGS_CIP,
             XW_OUTCOMES_CIP_CODE_4 = BGS_FINAL_CIP_CODE_4,
             XW_OUTCOMES_CIP_CODE_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
             XW_Final_Probable_Match = Final_Probable_Match),
    by = "ID"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2) &
        is.na(FINAL_CIP_CODE_2_NAME) & is.na(FINAL_CIP_CLUSTER_CODE) &
        is.na(FINAL_CIP_CLUSTER_NAME) & is.na(USE_BGS_CIP) & is.na(OUTCOMES_CIP_CODE_4) &
        is.na(OUTCOMES_CIP_CODE_4_NAME) & is.na(Final_Probable_Match),
      XW_FINAL_CIP_CODE_4, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4_NAME) & !is.na(XW_FINAL_CIP_CODE_4_NAME),
      XW_FINAL_CIP_CODE_4_NAME, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(is.na(FINAL_CIP_CODE_2) & !is.na(XW_FINAL_CIP_CODE_2),
                                XW_FINAL_CIP_CODE_2, FINAL_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = if_else(is.na(FINAL_CIP_CODE_2_NAME) & !is.na(XW_FINAL_CIP_CODE_2_NAME),
                                     XW_FINAL_CIP_CODE_2_NAME, FINAL_CIP_CODE_2_NAME),
    FINAL_CIP_CLUSTER_CODE = if_else(is.na(FINAL_CIP_CLUSTER_CODE) & !is.na(XW_FINAL_CIP_CLUSTER_CODE),
                                      XW_FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_CODE),
    FINAL_CIP_CLUSTER_NAME = if_else(is.na(FINAL_CIP_CLUSTER_NAME) & !is.na(XW_FINAL_CIP_CLUSTER_NAME),
                                      XW_FINAL_CIP_CLUSTER_NAME, FINAL_CIP_CLUSTER_NAME),
    USE_BGS_CIP = if_else(is.na(USE_BGS_CIP) & !is.na(XW_USE_BGS_CIP),
                           XW_USE_BGS_CIP, USE_BGS_CIP),
    OUTCOMES_CIP_CODE_4 = if_else(is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_OUTCOMES_CIP_CODE_4),
                                   XW_OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = if_else(is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_OUTCOMES_CIP_CODE_4_NAME),
                                        XW_OUTCOMES_CIP_CODE_4_NAME, OUTCOMES_CIP_CODE_4_NAME),
    Final_Probable_Match = if_else(is.na(Final_Probable_Match) & !is.na(XW_Final_Probable_Match),
                                   XW_Final_Probable_Match, Final_Probable_Match)
  ) %>%
  select(-starts_with("XW_"))

rm(xwalk_step1, xwalk_step2)


### Part 4B: Update Unmatched CIPs ----

# WHY: Records that still have no final CIP after XWALK matching use their STP
# CIP codes as the default. The "No because no match" flag indicates these
# weren't matched to BGS outcomes data.
Credential_Non_Dup_BGS_IDs <- Credential_Non_Dup_BGS_IDs %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_2) &
        is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      STP_CIP_CODE_4, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_2) &
        is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      STP_CIP_CODE_4_NAME, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      is.na(FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      STP_CIP_CODE_2, FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      is.na(FINAL_CIP_CODE_2_NAME) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      STP_CIP_CODE_2_NAME, FINAL_CIP_CODE_2_NAME
    ),
    USE_BGS_CIP = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_2) &
        is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      "No because no match", USE_BGS_CIP
    )
  ) %>%
  # Fill cluster codes from INFOWARE for any still NULL
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

# ---- Identify unmatched programs that could use BGS CIPs ----
# WHY: Some programs weren't matched in the XWALK but have BGS CIP data from
# other records in the same program. We identify these and update them.

# Programs using BGS CIPs (matched)
Credential_Matched_CIPS_using_BGS <- Credential_Non_Dup_BGS_IDs %>%
  filter(USE_BGS_CIP == "Yes") %>%
  count(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4_NAME,
        STP_CIP_CODE_4, STP_CIP_CODE_4_NAME, STP_CIP_CODE_2, STP_CIP_CODE_2_NAME,
        FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME, FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME,
        FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME,
        Final_Consider_A_Match, Final_Probable_Match, USE_BGS_CIP,
        name = "EXPR1")

# Programs not matched
Credential_Unmatched_CIPS <- Credential_Non_Dup_BGS_IDs %>%
  filter(USE_BGS_CIP == "No because no match") %>%
  count(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4_NAME,
        STP_CIP_CODE_4, STP_CIP_CODE_4_NAME, STP_CIP_CODE_2, STP_CIP_CODE_2_NAME,
        FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME, FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME,
        FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME,
        Final_Consider_A_Match, Final_Probable_Match, USE_BGS_CIP,
        name = "EXPR1")

# Find unmatched programs that were matched via BGS for different records
Credential_Unmatched_CIPS_to_review <- Credential_Unmatched_CIPS %>%
  select(-OUTCOMES_CIP_CODE_4, -OUTCOMES_CIP_CODE_4_NAME) %>%
  left_join(
    Credential_Matched_CIPS_using_BGS %>%
      distinct(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
               STP_CIP_CODE_4, OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4_NAME),
    by = c("PSI_CODE", "PSI_PROGRAM_CODE", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION", "STP_CIP_CODE_4")
  ) %>%
  mutate(
    Unmatched_But_in_BGS_Program = if_else(!is.na(OUTCOMES_CIP_CODE_4), "Yes", NA_character_),
    BGS_CIP_is_Different = if_else(OUTCOMES_CIP_CODE_4 != STP_CIP_CODE_4, "Yes", NA_character_)
  ) %>%
  group_by(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, STP_CIP_CODE_4) %>%
  filter(Unmatched_But_in_BGS_Program == "Yes" & BGS_CIP_is_Different == "Yes") %>%
  ungroup() %>%
  select(-PSI_CREDENTIAL_PROGRAM_DESCRIPTION, everything(), PSI_CREDENTIAL_PROGRAM_DESCRIPTION) %>%
  arrange(FINAL_CIP_CODE_4)

# ---- Year-specific custom updates ----
# WHY: Based on analyst review, these specific programs should use BGS CIPs
# even though they weren't matched in the XWALK. Update for each model run.
# !! UPDATE THIS TABLE FOR EACH MODEL RUN !!
Credential_Unmatched_CIPS_to_update <- tibble::tribble(
  ~PSI_CREDENTIAL_PROGRAM_DESCRIPTION,                                                  ~FINAL_CIP_CODE_4, ~FINAL_CIP_CODE_2,
  "Bachelor Of Applied Science In Mechatronic Systems Engineering",                     "1442",            "14",
  "Bachelor Of Athletic And Exercise Therapy",                                          "5123",            "51",
  "Bachelor Of Fine Arts In Dance",                                                     "5003",            "50",
  "Bachelor Of Fine Arts In Film",                                                      "5006",            "50",
  "Bachelor Of Fine Arts In Music - Composition",                                       "5009",            "50",
  "Bachelor Of Fine Arts In Music - Electroacoustic",                                   "5009",            "50",
  "Bachelor Of Fine Arts In Theatre - Performance",                                     "5005",            "50",
  "Bachelor Of Fine Arts In Theatre - Production And Design",                           "5005",            "50",
  "Bachelor Of Science In Geographic Information Science",                              "4507",            "45",
  "Bachelor Of Social Work In Indigenous Child Welfare",                                "4407",            "44",
  "Bachelor Of Social Work In Indigenous Social Work",                                  "4407",            "44",
  "Bachelor Of Child & Youth Care In Child & Youth Care",                               "1907",            "19",
  "Bachelor Of Child & Youth Care In Child & Youth Care - Child Life Stream",           "1907",            "19",
  "Bachelor Of Child & Youth Care In Child & Youth Care - Early Years Stream",          "1907",            "19",
  "Bachelor Of Child & Youth Care In Child & Youth Care - Child Protection",            "1907",            "19",
  "Bachelor Of Child & Youth Care In Child & Youth Care - Indigenous Stream",           "1907",            "19"
)

# Apply custom updates to unmatched programs
Credential_Non_Dup_BGS_IDs <- Credential_Non_Dup_BGS_IDs %>%
  left_join(
    Credential_Unmatched_CIPS_to_update %>%
      rename(UPD_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4, UPD_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2),
    by = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      !is.na(UPD_FINAL_CIP_CODE_4) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      UPD_FINAL_CIP_CODE_4, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      !is.na(UPD_FINAL_CIP_CODE_4) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      UPD_FINAL_CIP_CODE_2, FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(-UPD_FINAL_CIP_CODE_4, -UPD_FINAL_CIP_CODE_2)

# Re-fill CIP names and cluster codes from INFOWARE after updates
Credential_Non_Dup_BGS_IDs <- Credential_Non_Dup_BGS_IDs %>%
  left_join(
    cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("FINAL_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  mutate(FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME)) %>%
  select(-LCP4_CIP_4DIGITS_NAME) %>%
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_DIGITS_NAME, -LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

dbWriteTable(con, "Credential_Non_Dup_BGS_IDs", Credential_Non_Dup_BGS_IDs, overwrite = TRUE)

rm(Credential_Matched_CIPS_using_BGS, Credential_Unmatched_CIPS,
   Credential_Unmatched_CIPS_to_review, Credential_Unmatched_CIPS_to_update)


# ******************************************************************************
# PART 5: UPDATE T_BGS_DATA_FINAL
# ******************************************************************************
# WHY: Transfer the matched CIPs from the XWALK to the BGS outcomes data table.
# This mirrors Part 4 but for the BGS survey data rather than STP credentials.

### Part 5A: Update with XWALK ----

xwalk_step1 <- BGS_Matching_STP_Credential_PEN %>%
  filter(!is.na(Final_Consider_A_Match) & Final_Consider_A_Match != "")

xwalk_step2 <- BGS_Matching_STP_Credential_PEN %>%
  filter(!is.na(Final_Probable_Match) & Final_Probable_Match != "")

T_BGS_Data_Final_for_OutcomesMatching <- T_BGS_Data_Final_for_OutcomesMatching %>%
  # Add columns for final CIP data from XWALK
  mutate(
    STP_CIP_CODE_4 = NA_character_,
    STP_CIP_CODE_4_NAME = NA_character_,
    Final_Consider_A_Match = NA_character_,
    Final_Probable_Match = NA_character_,
    USE_BGS_CIP = NA_character_,
    USE_STP_CIP = NA_character_,
    FINAL_CIP_CODE_4 = NA_character_,
    FINAL_CIP_CODE_4_NAME = NA_character_,
    FINAL_CIP_CODE_2 = NA_character_,
    FINAL_CIP_CODE_2_NAME = NA_character_,
    FINAL_CIP_CLUSTER_CODE = NA_character_,
    FINAL_CIP_CLUSTER_NAME = NA_character_
  ) %>%
  # Step 1: Fill from auto-matched XWALK (Final_Consider_A_Match)
  left_join(
    xwalk_step1 %>%
      select(STQU_ID,
             XW_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
             XW_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
             XW_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
             XW_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
             XW_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
             XW_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
             XW_USE_BGS_CIP = USE_BGS_CIP,
             XW_STP_CIP_CODE_4 = STP_FINAL_CIP_CODE_4,
             XW_STP_CIP_CODE_4_NAME = STP_FINAL_CIP_CODE_4_NAME,
             XW_Final_Consider_A_Match = Final_Consider_A_Match,
             XW_Final_Probable_Match = Final_Probable_Match),
    by = "STQU_ID"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, XW_FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, XW_FINAL_CIP_CODE_4_NAME),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, XW_FINAL_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, XW_FINAL_CIP_CODE_2_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, XW_FINAL_CIP_CLUSTER_CODE),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, XW_FINAL_CIP_CLUSTER_NAME),
    USE_BGS_CIP = coalesce(USE_BGS_CIP, XW_USE_BGS_CIP),
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, XW_STP_CIP_CODE_4),
    STP_CIP_CODE_4_NAME = coalesce(STP_CIP_CODE_4_NAME, XW_STP_CIP_CODE_4_NAME),
    Final_Consider_A_Match = coalesce(Final_Consider_A_Match, XW_Final_Consider_A_Match),
    Final_Probable_Match = coalesce(Final_Probable_Match, XW_Final_Probable_Match)
  ) %>%
  select(-starts_with("XW_")) %>%
  # Step 2: Fill from manually matched XWALK (Final_Probable_Match)
  left_join(
    xwalk_step2 %>%
      select(STQU_ID,
             XW_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
             XW_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
             XW_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
             XW_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
             XW_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
             XW_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
             XW_USE_BGS_CIP = USE_BGS_CIP,
             XW_STP_CIP_CODE_4 = STP_FINAL_CIP_CODE_4,
             XW_STP_CIP_CODE_4_NAME = STP_FINAL_CIP_CODE_4_NAME,
             XW_Final_Probable_Match = Final_Probable_Match),
    by = "STQU_ID"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2) &
        is.na(FINAL_CIP_CODE_2_NAME) & is.na(FINAL_CIP_CLUSTER_CODE) &
        is.na(FINAL_CIP_CLUSTER_NAME) & is.na(USE_BGS_CIP) & is.na(STP_CIP_CODE_4) &
        is.na(STP_CIP_CODE_4_NAME) & is.na(Final_Probable_Match) &
        !is.na(XW_FINAL_CIP_CODE_4),
      XW_FINAL_CIP_CODE_4, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, XW_FINAL_CIP_CODE_4_NAME),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, XW_FINAL_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, XW_FINAL_CIP_CODE_2_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, XW_FINAL_CIP_CLUSTER_CODE),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, XW_FINAL_CIP_CLUSTER_NAME),
    USE_BGS_CIP = coalesce(USE_BGS_CIP, XW_USE_BGS_CIP),
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, XW_STP_CIP_CODE_4),
    STP_CIP_CODE_4_NAME = coalesce(STP_CIP_CODE_4_NAME, XW_STP_CIP_CODE_4_NAME),
    Final_Probable_Match = coalesce(Final_Probable_Match, XW_Final_Probable_Match)
  ) %>%
  select(-starts_with("XW_")) %>%
  # Convert USE_BGS_CIP to USE_STP_CIP (inverse logic)
  mutate(USE_STP_CIP = case_when(
    USE_BGS_CIP == "Yes" ~ "No",
    USE_BGS_CIP == "No" ~ "Yes",
    TRUE ~ NA_character_
  )) %>%
  select(-USE_BGS_CIP)

rm(xwalk_step1, xwalk_step2)


### Part 5B: Update Unmatched CIPs ----

# WHY: BGS records without a match in the XWALK use their own BGS CIP codes.
# This is the inverse of Part 4B where unmatched STP records used STP CIPs.
T_BGS_Data_Final_for_OutcomesMatching <- T_BGS_Data_Final_for_OutcomesMatching %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_2) &
        is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      CIP_4DIGIT_NO_PERIOD, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2) &
        is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      CIP4DIG_NAME, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      is.na(FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      CIP2DIG, FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      is.na(FINAL_CIP_CODE_2_NAME) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      CIP2DIG_NAME, FINAL_CIP_CODE_2_NAME
    ),
    USE_STP_CIP = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_2) &
        is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      "No because no match", USE_STP_CIP
    )
  ) %>%
  # Fill cluster codes from INFOWARE
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

# ---- Identify unmatched BGS programs that could use STP CIPs ----
T_BGS_Data_Matched_CIPS_using_STP <- T_BGS_Data_Final_for_OutcomesMatching %>%
  filter(USE_STP_CIP == "Yes") %>%
  count(INSTITUTION_CODE, CPC, PROGRAM,
        STP_CIP_CODE_4, STP_CIP_CODE_4_NAME,
        CIP_4DIGIT_NO_PERIOD, CIP4DIG_NAME, CIP2DIG, CIP2DIG_NAME,
        FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME, FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME,
        FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME,
        Final_Consider_A_Match, Final_Probable_Match, USE_STP_CIP,
        name = "EXPR1")

T_BGS_Data_Unmatched_CIPS <- T_BGS_Data_Final_for_OutcomesMatching %>%
  filter(USE_STP_CIP == "No because no match") %>%
  count(INSTITUTION_CODE, CPC, PROGRAM,
        STP_CIP_CODE_4, STP_CIP_CODE_4_NAME,
        CIP_4DIGIT_NO_PERIOD, CIP4DIG_NAME, CIP2DIG, CIP2DIG_NAME,
        FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME, FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME,
        FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME,
        Final_Consider_A_Match, Final_Probable_Match, USE_STP_CIP,
        name = "EXPR1")

# Find unmatched programs that have STP matches for different records
T_BGS_Data_Unmatched_CIPS_to_review <- T_BGS_Data_Unmatched_CIPS %>%
  select(-STP_CIP_CODE_4, -STP_CIP_CODE_4_NAME) %>%
  left_join(
    T_BGS_Data_Matched_CIPS_using_STP %>%
      distinct(INSTITUTION_CODE, CPC, PROGRAM, CIP_4DIGIT_NO_PERIOD,
               STP_CIP_CODE_4, STP_CIP_CODE_4_NAME),
    by = c("INSTITUTION_CODE", "CPC", "PROGRAM", "CIP_4DIGIT_NO_PERIOD")
  ) %>%
  mutate(
    Unmatched_But_in_STP_Program = if_else(!is.na(STP_CIP_CODE_4), "Yes", NA_character_),
    STP_CIP_is_Different = if_else(STP_CIP_CODE_4 != CIP_4DIGIT_NO_PERIOD, "Yes", NA_character_)
  ) %>%
  group_by(INSTITUTION_CODE, CPC, PROGRAM, CIP_4DIGIT_NO_PERIOD) %>%
  filter(Unmatched_But_in_STP_Program == "Yes" & STP_CIP_is_Different == "Yes") %>%
  ungroup() %>%
  select(-PROGRAM, everything(), PROGRAM) %>%
  arrange(FINAL_CIP_CODE_4)

# ---- Year-specific custom updates for BGS data ----
# !! UPDATE THIS TABLE FOR EACH MODEL RUN !!
T_BGS_Data_Unmatched_CIPS_to_update <- tibble::tribble(
  ~PROGRAM,                                                                   ~FINAL_CIP_CODE_4, ~FINAL_CIP_CODE_2,
  "Bachelor of Applied Science - Mechatronic Systems Engineering Major",      "1442",            "14",
  "Bachelor of Applied Science In Chemical Engineering",                       "1407",            "14",
  "Bachelor of Applied Science In Chemical Engineering Minor In Commerce",     "1407",            "14",
  "Bachelor of Applied Science In Chemical Engineering Option in Biology",     "1407",            "14",
  "Bachelor of Environment - Resource and Environmental Management Major",     "0301",            "03",
  "Bachelor of Environment - Resource and Environmental Management Major, First Nations Studies Minor", "0301", "03",
  "Bachelor of Environment - Resource and Environmental Management Major, Geography Minor", "0301", "03",
  "Bachelor of Science - Biomedical Physiology Major",                         "2609",            "26",
  "Bachelor of Science in Applied Psychology",                                 "4228",            "42"
)

# Apply custom updates
T_BGS_Data_Final_for_OutcomesMatching <- T_BGS_Data_Final_for_OutcomesMatching %>%
  left_join(
    T_BGS_Data_Unmatched_CIPS_to_update %>%
      rename(UPD_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4, UPD_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2),
    by = "PROGRAM"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      !is.na(UPD_FINAL_CIP_CODE_4) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      UPD_FINAL_CIP_CODE_4, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      !is.na(UPD_FINAL_CIP_CODE_4) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      UPD_FINAL_CIP_CODE_2, FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(-UPD_FINAL_CIP_CODE_4, -UPD_FINAL_CIP_CODE_2)

# Re-fill CIP names and cluster codes from INFOWARE
T_BGS_Data_Final_for_OutcomesMatching <- T_BGS_Data_Final_for_OutcomesMatching %>%
  left_join(
    cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("FINAL_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  mutate(FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME)) %>%
  select(-LCP4_CIP_4DIGITS_NAME) %>%
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_DIGITS_NAME, -LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

dbWriteTable(con, "T_BGS_Data_Final_for_OutcomesMatching",
             T_BGS_Data_Final_for_OutcomesMatching, overwrite = TRUE)

rm(T_BGS_Data_Matched_CIPS_using_STP, T_BGS_Data_Unmatched_CIPS,
   T_BGS_Data_Unmatched_CIPS_to_review, T_BGS_Data_Unmatched_CIPS_to_update)


# ---- Clean up ----

dbDisconnect(con)
