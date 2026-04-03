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

# WHAT: This script performs program matching between BGS survey outcomes and STP credential data.
# We need to match Baccalaureate Graduate Survey (BGS) data from the Student Outcomes team, to Student Transitions Project (STP) Credential data. The two sources have many differences in the program names submitted, which results in differences in the Classification of Instructional Programs (CIP) codes used in the modelling. There are no College Program Codes (CPCs) in BGS data like there are in the Diploma, Associate Degree and Certificate Survey (DACSO) data so instead we have to do case level matching to determine what the CIP code differences are between STP and BGS data, and decide which CIP to use. In general, we keep the STP CIP [SY1.1]if we are not able match at the case-level to compare.
# WHY: BGS survey data uses INFOWARE CIP codes while STP uses different CIP classifications.
#      We need to align these codes to enable accurate supply modeling across data sources.
# HOW: 1) Build combined outcomes data from INFOWARE tables
#      2) Create normalized STP CIP codes (4D and 2D) from credential data
#      3) Match records by PEN (Personal Education Number)
#      4) Apply flagging logic for high-confidence matches
#      5) Update source tables with matched CIP codes
# The general rules for CIP coding the BGS data are:[SY2.1]
# •	If records matched on PEN, institution, award year, and CIP 4-digit, then we used the BGS CIP (which is the same as the STP CIP). No CIP change required.
# •	If records matched on PEN, institution, award year, CIP 2-digit (but had a different CIP 4-digit), then historically would used the STP CIP. In 2023, some new criteria were added as work has been done to improve the BGS CIPs:
#    o	If the BGS CIP is in a “general program” (the 4-digit CIP category is the 2-digit CIP category followed by “general”) – then use the STP CIP
#    o	If the STP CIP is in a “general program” – then use the BGS CIP
#    o	Then using the 4-digit matches, if one of the 2-digit program matches can be linked to the 4-digit matches on STP institution, program, and CIP – then use the STP CIP
#    o	If on of the 2-digit program matches can be linked to the 4-digit matches on BGS institution, program and CIP – then use the BGS CIP
#    o	Some additional custom CIP choices were made
#    o	The remaining were mostly double majors – used STP CIP for these
# •	If records matched on PEN, institution, and award year only, but had a different CIP 4-digit and 2-digit, then we manually investigated at the case level and decided if it was really a match, and if so whether the BGS or the STP CIP was the most appropriate to use.
# •	For the remaining unmatched records, use the STP CIP in the Credential data and the BGS CIP in the BGS data. This is where there will still be differences between BGS data and STP.

# ******************************************************************************
# Aligns CIP codes between BGS survey and STP data
# Required Tables
#   INFOWARE_BGS_DIST_19_23 - BGS outcomes 2019-2023
#   INFOWARE_BGS_DIST_18_22 - BGS outcomes 2018-2022
#   INFOWARE_BGS_COHORT_INFO - Cohort metadata for matching
#   INFOWARE_L_CIP_6DIGITS_CIP2016 - 6-digit CIP lookup
#   INFOWARE_L_CIP_4DIGITS_CIP2016 - 4-digit CIP lookup
#   INFOWARE_L_CIP_2DIGITS_CIP2016 - 2-digit CIP lookup
#   Credential_non_Dup - STP credential data with CIP codes
#   STP_Credential - for PSI_PEN (institution identifier)
# Resulting Tables
#   T_BGS_Data_Final_for_OutcomesMatching - Combined BGS outcomes data
#   Credential_Non_Dup_BGS_IDs - BGS credentials with matched CIPs
#   Credential_Non_Dup_GRAD_IDs - GRAD credentials with matched CIPs
#
# WHAT: This script performs program matching between BGS survey outcomes and STP credential data.
# WHY: BGS survey data uses INFOWARE CIP codes while STP uses different CIP classifications.
#      We need to align these codes to enable accurate supply modeling across data sources.
# HOW: 1) Build combined outcomes data from INFOWARE tables
#      2) Create normalized STP CIP codes (4D and 2D) from credential data
#      3) Match records by PEN (Personal Education Number)
#      4) Apply flagging logic for high-confidence matches
#      5) Update source tables with matched CIP codes
#

############################################################################
# oracle connection instruction
# Follow this solution: [\[oracle.com\]](https://www.oracle.com/database/technologies/releasenote-odbc-ic.html), [\[oracle.com\]](https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html)

# 1.  Install **Oracle Instant Client 19c Basic** and **19c ODBC** into something like `C:\Oracle\instantclient_19_30`. [\[oracle.com\]](https://www.oracle.com/database/technologies/releasenote-odbc-ic.html), [\[oracle.com\]](https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html)
# 2.  Add that folder to **PATH** and put it **before** old Oracle 11g folders. [\[oracle.com\]](https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html)
# 3.  Set **`TNS_ADMIN`** to the folder containing your `tnsnames.ora`. [\[rdrr.io\]](https://rdrr.io/cran/DBI/man/dbBind.html), [\[stackoverflow.com\]](https://stackoverflow.com/questions/50750812/how-to-get-sid-service-name-and-port-for-oracle-database)
# 4.  Run **`odbc_install.exe`**. [\[oracle.com\]](https://www.oracle.com/database/technologies/releasenote-odbc-ic.html)
# 5.  Restart RStudio. [\[oracle.com\]](https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html)
# 6.  Run `odbcListDrivers()` and copy the exact driver name into your R code. [\[docs.oracle.com\]](https://docs.oracle.com/en/database/oracle/oracle-database/23/odbcd/basic-programming-oracle-odbc.html), [\[quantargo.com\]](https://www.quantargo.com/help/r/latest/packages/DBI/html/transactions)
# ############################################################################

options(java.parameters = " -Xmx102400m") ## For reading oracle tables: increase amount of memory java is allowed to use

library(tidyverse)
library(RODBC)
library(odbc)
library(DBI)
library(glue)
library(RJDBC)
library(dbplyr)


# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

# Connect to Decimal
con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)


# ---- Read in INFOWARE tables ----

iw_config <- config::get("infoware")

odbcListDrivers()

iw_con <- dbConnect(
  odbc::odbc(),
  Driver = "Oracle in instantclient_19_30",
  DBQ = "DEV01.world",
  UID = iw_config$uid,
  PWD = iw_config$pwd
)


## ** NOTE **
## Ideally match on all data but prioritizing the most recent 6 years - see documentation
## Update which BGS_DIST tables to include.

## Run the following to get a list of all tables available
# alltables_Infoware <- dbReadTable(iw_con,"ALL_TABLES")

INFOWARE_BGS_DIST_19_23 <- dbReadTable(
  iw_con,
  DBI::Id(schema = "INFOWARE", table = "BGS_DIST_19_23")
)

INFOWARE_BGS_DIST_18_22 <- dbReadTable(
  iw_con,
  DBI::SQL("INFOWARE.BGS_DIST_18_22")
)
INFOWARE_BGS_COHORT_INFO <- dbReadTable(
  iw_con,
  DBI::SQL("INFOWARE.BGS_COHORT_INFO")
)

INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbReadTable(
  iw_con,
  DBI::Id(schema = "INFOWARE", table = "L_CIP_6DIGITS_CIP2016")
)
INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbReadTable(
  iw_con,
  DBI::Id(schema = "INFOWARE", table = "L_CIP_4DIGITS_CIP2016")
)
INFOWARE_L_CIP_2DIGITS_CIP2016 <- dbReadTable(
  iw_con,
  DBI::Id(schema = "INFOWARE", table = "L_CIP_2DIGITS_CIP2016")
)

dbDisconnect(iw_con)

# ---- Write initial tables to Decimal ----
## Save static versions of the INFOWARE tables and last cycle XWALK to Decimal
# !! UPDATE THE TABLES AND ROW NUMBERS !! - connection won't write the full datasets to decimal due to size
dbWriteTable(con, "INFOWARE_BGS_DIST_19_23", INFOWARE_BGS_DIST_19_23[1:80000, ])
dbWriteTable(
  con,
  "INFOWARE_BGS_DIST_19_23",
  INFOWARE_BGS_DIST_19_23[80001:121074, ],
  append = TRUE
)
dbWriteTable(con, "INFOWARE_BGS_DIST_18_22", INFOWARE_BGS_DIST_18_22[1:80000, ])
dbWriteTable(
  con,
  "INFOWARE_BGS_DIST_18_22",
  INFOWARE_BGS_DIST_18_22[80001:118632, ],
  append = TRUE
)
dbWriteTable(
  con,
  "INFOWARE_BGS_COHORT_INFO",
  INFOWARE_BGS_COHORT_INFO[1:80000, ]
)
dbWriteTable(
  con,
  "INFOWARE_BGS_COHORT_INFO",
  INFOWARE_BGS_COHORT_INFO[80001:160000, ],
  append = TRUE
)
dbWriteTable(
  con,
  "INFOWARE_BGS_COHORT_INFO",
  INFOWARE_BGS_COHORT_INFO[160001:240000, ],
  append = TRUE
)
dbWriteTable(
  con,
  "INFOWARE_BGS_COHORT_INFO",
  INFOWARE_BGS_COHORT_INFO[240001:290758, ],
  append = TRUE
)
dbWriteTable(
  con,
  DBI::Id(schema = my_schema, table = "INFOWARE_L_CIP_6DIGITS_CIP2016"),
  INFOWARE_L_CIP_6DIGITS_CIP2016
)
dbWriteTable(
  con,
  DBI::Id(schema = my_schema, table = "INFOWARE_L_CIP_4DIGITS_CIP2016"),
  INFOWARE_L_CIP_4DIGITS_CIP2016
)
dbWriteTable(
  con,
  "INFOWARE_L_CIP_2DIGITS_CIP2016",
  INFOWARE_L_CIP_2DIGITS_CIP2016
)

## check tables loaded correctly
{
  nrow <- tbl(con, "INFOWARE_BGS_DIST_19_23") %>% tally()
  nrow ## how many rows?
  tbl(con, "INFOWARE_BGS_DIST_19_23") %>% distinct(STQU_ID) %>% tally() ## are all IDs unique?

  nrow <- tbl(con, "INFOWARE_BGS_DIST_18_22") %>% tally()
  nrow ## how many rows?
  tbl(con, "INFOWARE_BGS_DIST_18_22") %>% distinct(STQU_ID) %>% tally() ## are all IDs unique?

  nrow <- tbl(con, "INFOWARE_BGS_COHORT_INFO") %>% tally()
  nrow ## how many rows?
  tbl(con, "INFOWARE_BGS_COHORT_INFO") %>% distinct(STQU_ID) %>% tally() ## are all IDs unique?

  rm(nrow)
}

## remove tables and use decimal versions for remainder of code
rm(
  INFOWARE_BGS_DIST_19_23,
  INFOWARE_BGS_DIST_18_22,
  INFOWARE_BGS_COHORT_INFO,
  INFOWARE_L_CIP_6DIGITS_CIP2016,
  INFOWARE_L_CIP_4DIGITS_CIP2016,
  INFOWARE_L_CIP_2DIGITS_CIP2016
)


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

missing_tables <- required_tables[
  !map_lgl(
    required_tables,
    ~ dbExistsTable(con, Id(schema = my_schema, table = .x))
  )
]

if (length(missing_tables) > 0) {
  stop(glue::glue(
    "The following required tables are missing in schema '{my_schema}': {paste(missing_tables, collapse = ', ')}. Please run 'R/load-infoware-lookups.R' first."
  ))
}

# ---- Table References ----
infoware_bgs_19_23 <- tbl(con, in_schema(my_schema, "INFOWARE_BGS_DIST_19_23"))
infoware_bgs_18_22 <- tbl(con, in_schema(my_schema, "INFOWARE_BGS_DIST_18_22"))
infoware_cohort_info <- tbl(
  con,
  in_schema(my_schema, "INFOWARE_BGS_COHORT_INFO")
)

cip_6_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_6DIGITS_CIP2016"))
cip_4_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_4DIGITS_CIP2016"))
cip_2_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_2DIGITS_CIP2016"))

credential_non_dup_tbl <- tbl(con, in_schema(my_schema, "credential_non_dup"))
stp_credential_tbl <- tbl(con, in_schema(my_schema, "STP_Credential"))


# id should be unique for updates to be reliable.
infoware_cohort_info |> tally()
# 290758
infoware_cohort_info %>%
  count(PEN) %>%
  filter(n > 1) %>%
  tally()
# NO
infoware_cohort_info %>%
  count(STUDID) %>%
  filter(n > 1) %>%
  tally()
# NO
infoware_cohort_info %>%
  count(STQU_ID) %>%
  filter(n > 1) %>%
  tally()
# YES

## Part 1: Build Outcomes Data ----
## Created tables: T_BGS_Data_Final_for_OutcomesMatching

# BGS data: Build T_DATA_FINAL_for_OutcomesMatching table with past 6 years
## ** IMPORTANT - update queries with table years **
#
# WHAT: Combines BGS outcomes data from two time periods (2019-2023 and 2018-2022) into a unified table.
# WHY: Data is split across two INFOWARE tables with different year ranges. We need to union them
#      and standardize column names for downstream matching logic.
# HOW: 1) Inner join each outcomes table with cohort info to get institution metadata
#      2) Select common columns including PEN, institution codes, CIP codes, and program info
#      3) Union the two datasets and add PSSM_CREDENTIAL identifier
#      4) Materialize as persistent table for subsequent steps

# Step 1: 2020 Outcomes (from 19_23 table)
t_bgs_step1 <- infoware_bgs_19_23 %>%
  select(
    STQU_ID,
    RESPONDENT,
    YEAR,
    INSTITUTION_CODE,
    INSTITUTION
  ) |>
  inner_join(
    infoware_cohort_info |>
      select(
        PEN,
        STUDID,
        STQU_ID,
        SRV_Y_N,
        SUBM_CD,
        CIP2DIG,
        CIP2DIG_NAME,
        CIP4DIG,
        CIP_4DIGIT_NO_PERIOD,
        CIP4DIG_NAME,
        CIP_6DIGIT_1,
        CIP_6DIGIT_NO_PERIOD,
        CIP6DIG_NAME,
        PROGRAM,
        DASHBOARD_PROGRAM,
        CPC
      ),
    by = "STQU_ID"
  ) %>%
  select(
    PEN,
    STUDID,
    STQU_ID,
    SRV_Y_N,
    RESPONDENT,
    YEAR,
    INSTITUTION_CODE,
    INSTITUTION,
    SUBM_CD,
    CIP2DIG,
    CIP2DIG_NAME,
    CIP4DIG,
    CIP_4DIGIT_NO_PERIOD,
    CIP4DIG_NAME,
    CIP_6DIGIT_1,
    CIP_6DIGIT_NO_PERIOD,
    CIP6DIG_NAME,
    PROGRAM,
    DASHBOARD_PROGRAM,
    CPC
  )

# Step 2: 2018 Outcomes (from 18_22 table, filtered for Year 2018)
t_bgs_step2 <- infoware_bgs_18_22 %>%
  filter(YEAR == 2018) %>%
  select(
    STQU_ID,
    RESPONDENT,
    YEAR,
    INSTITUTION_CODE,
    INSTITUTION
  ) |>
  inner_join(
    infoware_cohort_info |>
      select(
        PEN,
        STUDID,
        STQU_ID,
        SRV_Y_N,
        SUBM_CD,
        CIP2DIG,
        CIP2DIG_NAME,
        CIP4DIG,
        CIP_4DIGIT_NO_PERIOD,
        CIP4DIG_NAME,
        CIP_6DIGIT_1,
        CIP_6DIGIT_NO_PERIOD,
        CIP6DIG_NAME,
        PROGRAM,
        DASHBOARD_PROGRAM,
        CPC
      ),
    by = "STQU_ID"
  ) %>%
  select(
    PEN,
    STUDID,
    STQU_ID,
    SRV_Y_N,
    RESPONDENT,
    YEAR,
    SUBM_CD,
    INSTITUTION_CODE,
    INSTITUTION,
    CIP2DIG,
    CIP2DIG_NAME,
    CIP4DIG,
    CIP_4DIGIT_NO_PERIOD,
    CIP4DIG_NAME,
    CIP_6DIGIT_1,
    CIP_6DIGIT_NO_PERIOD,
    CIP6DIG_NAME,
    PROGRAM,
    DASHBOARD_PROGRAM,
    CPC
  )

# Combine and Add PSSM_CREDENTIAL

t_bgs_final <- union_all(t_bgs_step1, t_bgs_step2) %>%
  mutate(PSSM_CREDENTIAL = "BACH") %>%
  {
    if (
      dbExistsTable(
        con,
        Id(schema = my_schema, table = "T_BGS_Data_Final_for_OutcomesMatching")
      )
    ) {
      dbRemoveTable(
        con,
        Id(schema = my_schema, table = "T_BGS_Data_Final_for_OutcomesMatching")
      )
    }
    .
  } %>%
  compute(
    name = Id(
      schema = my_schema,
      table = "T_BGS_Data_Final_for_OutcomesMatching"
    ),
    temporary = FALSE
  )

# id should be unique for updates to be reliable.
t_bgs_final |> tally()
# 143811
t_bgs_final %>%
  count(PEN) %>%
  filter(n > 1) %>%
  tally()

t_bgs_final %>%
  count(STUDID) %>%
  filter(n > 1) %>%
  tally()

t_bgs_final %>%
  count(STQU_ID) %>%
  filter(n > 1) %>%
  tally()


# To uniquely join two tables,  use STQU_ID as key, don't use PEN or STUDID.

t_bgs_final <- tbl(
  con,
  Id(
    schema = my_schema,
    table = "T_BGS_Data_Final_for_OutcomesMatching"
  )
)


## check counts by year
{
  t_bgs_final %>%
    group_by(YEAR) %>%
    tally()
}

## check for any NAs in the 2,4,6 CIPS- try to fix NAs if they exist
{
  chk_bgs_cip <- t_bgs_final %>%
    group_by(
      CIP2DIG,
      CIP2DIG_NAME,
      CIP4DIG,
      CIP4DIG_NAME,
      CIP_6DIGIT_NO_PERIOD,
      CIP6DIG_NAME
    ) %>%
    summarize(N = n(), .groups = "drop") %>%
    collect()
  chk_bgs_cip %>% filter(if_any(everything(), is.na))
  # no data, so pass
  rm(chk_bgs_cip)
}

# ---- Part 2: Create Credential (STP) 4D and 2D CIP Codes ----
# Created tables: Credential_Non_Dup_BGS_IDs, Credential_Non_Dup_GRAD_IDs
#
# WHAT: Normalizes CIP codes from STP credential data into 4-digit and 2-digit formats matching BGS structure.
# WHY: STP credentials use 6-digit CIP codes with periods (e.g., "51.0204") while BGS uses 4-digit
#      codes without periods (e.g., "5102"). We need consistent formats for matching.
# HOW: 1) Extract BGS/GRAD credentials and clean CIP strings (fix leading/trailing zeros)
#      2) Join to CIP lookup tables to derive 4D and 2D codes from 6-digit codes
#      3) Apply fallback logic: 5-char match, then 2-char match, then general program defaults
#      4) Join to CIP name tables for human-readable descriptions
#      5) Create separate ID tables for BGS (needs PEN matching) and GRAD (direct finalize)
#

# 1. Create cleaning table (collect STP BGS/GRAD data)
stp_cip_cleaning <- credential_non_dup_tbl |>
  rename_with(toupper) |>
  filter(OUTCOMES_CRED %in% c("BGS", "GRAD")) |>
  group_by(PSI_CREDENTIAL_CIP, OUTCOMES_CRED) |>
  summarize(Expr1 = n(), .groups = "drop") |>
  mutate(PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP) |>
  mutate(
    PSI_CREDENTIAL_CIP = case_when(
      # clean PSI_CREDENTIAL_CIP that don't have 7 characters (i.e., not in the format XX.XXXX)
      ## update PSI_CREDENTIAL_CIP to add leading zero when formatted as X.XXXX
      nchar(PSI_CREDENTIAL_CIP) == 6 & substr(PSI_CREDENTIAL_CIP, 2, 2) == "." ~
        paste0("0", PSI_CREDENTIAL_CIP),
      ## update PSI_CREDENTIAL_CIP to add trailing zero when formatted as XX.XXX
      nchar(PSI_CREDENTIAL_CIP) == 6 & substr(PSI_CREDENTIAL_CIP, 2, 2) != "." ~
        paste0(PSI_CREDENTIAL_CIP, "0"),
      TRUE ~ PSI_CREDENTIAL_CIP
    ),
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

## New: Add 4D CIP codes for general programs (if 00 change to 01)
## Check which CIPs have general programs here: https://www.statcan.gc.ca/en/subjects/standard/cip/2021/index
# Recode general programs
general_cips <- c(
  "11.00",
  "13.00",
  "14.00",
  "19.00",
  "23.00",
  "24.00",
  "26.00",
  "40.00",
  "42.00",
  "45.00",
  "50.00",
  "52.00",
  "55.00"
)
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

# 3. Add 4D and 2D CIP names
stp_cip_cleaning <- stp_cip_cleaning %>%
  left_join(
    cip_4_tbl %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("STP_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  left_join(
    cip_2_tbl %>% select(LCP2_CD, LCP2_DIGITS_NAME),
    by = c("STP_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  rename(
    STP_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME,
    STP_CIP_CODE_2_NAME = LCP2_DIGITS_NAME
  ) %>%
  mutate(
    STP_CIP_CODE_4_NAME = coalesce(STP_CIP_CODE_4_NAME, "Invalid 4-digit CIP")
  ) %>%
  {
    if (
      dbExistsTable(
        con,
        Id(schema = my_schema, table = "Credential_Non_Dup_STP_CIP4_Cleaning")
      )
    ) {
      dbRemoveTable(
        con,
        Id(schema = my_schema, table = "Credential_Non_Dup_STP_CIP4_Cleaning")
      )
    }
    .
  } %>%
  compute(
    name = Id(
      schema = my_schema,
      table = "Credential_Non_Dup_STP_CIP4_Cleaning"
    ),
    temporary = FALSE
  )

stp_cip_cleaning <- tbl(
  con,
  Id(
    schema = my_schema,
    table = "Credential_Non_Dup_STP_CIP4_Cleaning"
  )
)

stp_cip_cleaning |> tally()
# 681

## check correct version of Credential_Non_Dup used
{
  t1 <- tbl(con, dbplyr::in_schema("dbo", "credential_non_dup")) %>%
    select(
      PSI_CREDENTIAL_CIP = psi_credential_cip,
      OUTCOMES_CRED = outcomes_cred
    ) %>%
    filter(OUTCOMES_CRED %in% c("BGS", "GRAD")) %>%
    group_by(PSI_CREDENTIAL_CIP, OUTCOMES_CRED) %>%
    summarize(Expr1 = n(), .groups = "drop") %>%
    collect() %>%
    arrange(PSI_CREDENTIAL_CIP, OUTCOMES_CRED)

  t2 <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>%
    collect() %>%
    arrange(PSI_CREDENTIAL_CIP, OUTCOMES_CRED)

  all.equal(t1, t2 |> select(1:3))

  rm(t1, t2)
}

## check PSI_CREDENTIAL_CIP to make sure they are formatted correctly
## i.e., in the format XX.XXXX
{
  chk <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>%
    distinct(PSI_CREDENTIAL_CIP_orig) %>%
    collect()

  chk %>% filter(nchar(PSI_CREDENTIAL_CIP_orig) != 7)
  # zero row, so pass.
  rm(chk)
}

## check for any non-matches
{
  chk1 <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>%
    filter(is.na(STP_CIP_CODE_4)) %>%
    collect()
  # zero rows, so pass
}

## check update and any non-matches: chk1 has no data
{
  if (nrow(chk1) > 0) {
    chk2 <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>%
      filter(PSI_CREDENTIAL_CIP %in% chk1$PSI_CREDENTIAL_CIP) %>%
      collect()

    chk2 <- chk2 %>% filter(is.na(STP_CIP_CODE_4))

    ## check update and any non-matches: chk2 has no data
    {
      if (nrow(chk2) > 0) {
        chk3 <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>%
          filter(PSI_CREDENTIAL_CIP %in% chk2$PSI_CREDENTIAL_CIP) %>%
          collect()

        chk3 <- chk3 %>% filter(is.na(STP_CIP_CODE_4))
      }
    }
  }
}


# Check for blank CIP4s and CIP2s: no data, so pass
{
  chk <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>%
    filter(is.na(STP_CIP_CODE_4) | is.na(STP_CIP_CODE_2)) %>%
    collect() ## populate CIP4 and CIP2 that are blank (add to custom code above)

  rm(chk, chk1, chk2, chk3)
}

## review table: 6 rows are all general programs, due to previously we Add 4D CIP codes for general programs
{
  chk <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>% collect()
  chk1 <- chk %>%
    filter(
      STP_CIP_CODE_4 !=
        str_sub(PSI_CREDENTIAL_CIP_orig, end = 5) %>% str_remove_all("\\.")
    )

  rm(chk, chk1)
}

# ---- Part 2 (continued): Create BGS and GRAD credential ID tables ----
# WHAT: Splits STP credential data into two separate tables: one for BGS credentials (which will undergo
#       matching to BGS survey outcomes), and one for GRAD credentials (which use STP CIPs directly).
# WHY: BGS and GRAD credentials follow different downstream processing paths. BGS credentials need to be
#      matched to survey outcomes to determine whether to use BGS or STP CIP codes. GRAD credentials
#      are finalized immediately with STP CIP codes (no BGS survey matching available).
# HOW: 1) Join cleaned STP CIP codes (from stp_cip_cleaning) back to credential base data
#      2) Filter for BGS vs GRAD credential types
#      3) For BGS: add PSI_PEN from STP_Credential table for later PEN-based matching to survey data
#      4) For GRAD: finalize CIP columns immediately (STP CIP becomes FINAL_CIP)
#      5) Materialize both tables as persistent SQL tables for downstream use
#

# Create base table: join cleaned CIP codes to credential data
# This combines the 4D and 2D normalized STP CIP codes with institution, program, and award year info
stp_cip_ids <- credential_non_dup_tbl %>%
  rename_with(toupper) |>
  filter(OUTCOMES_CRED %in% c("BGS", "GRAD")) %>%
  select(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED
  ) |>
  inner_join(
    stp_cip_cleaning |>
      select(
        PSI_CREDENTIAL_CIP_orig,
        OUTCOMES_CRED,
        STP_CIP_CODE_4,
        STP_CIP_CODE_4_NAME,
        STP_CIP_CODE_2,
        STP_CIP_CODE_2_NAME
      ),
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_orig", "OUTCOMES_CRED")
  )

stp_cip_ids |>
  filter(ID == "849715")

# ---- BGS Credentials: Credential_Non_Dup_BGS_IDs ----
# Create a table with only BGS credentials and normalized STP CIP codes (485925 rows in 2023)
# These records will later be matched to BGS survey outcomes (T_BGS_Data_Final_for_OutcomesMatching)
# using PEN as the primary key. The matching process determines whether to use BGS or STP CIP codes.

bgs_ids_base <- stp_cip_ids %>%
  filter(OUTCOMES_CRED == "BGS") %>%
  select(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME,
    STP_CIP_CODE_2,
    STP_CIP_CODE_2_NAME
  ) %>%
  # Clean up NULL program codes that were imported as "(Unspecified)"
  mutate(
    PSI_PROGRAM_CODE = if_else(
      PSI_PROGRAM_CODE == "(Unspecified)",
      NA_character_,
      PSI_PROGRAM_CODE
    )
  )

bgs_ids_base |> tally() # verify count matches expected from documentation
# 485925
# Add PSI_PEN (Personal Education Number) from STP_Credential table
# PSI_PEN is the linking key between STP credentials and BGS survey outcomes.
# This join brings in the PEN identifier needed for Part 3 (matching BGS survey data to credentials)
credential_bgs_ids <- bgs_ids_base %>%
  left_join(stp_credential_tbl %>% select(ID, PSI_PEN), by = "ID") %>%
  # Materialize as persistent table for use in later steps (Part 3: matching to BGS outcomes)
  {
    if (
      dbExistsTable(
        con,
        Id(schema = my_schema, table = "Credential_Non_Dup_BGS_IDs")
      )
    ) {
      dbRemoveTable(
        con,
        Id(schema = my_schema, table = "Credential_Non_Dup_BGS_IDs")
      )
    }
    .
  } %>%
  compute(
    name = Id(schema = my_schema, table = "Credential_Non_Dup_BGS_IDs"),
    temporary = FALSE
  )

credential_bgs_ids <- tbl(
  con,
  Id(schema = my_schema, table = "Credential_Non_Dup_BGS_IDs")
)
credential_bgs_ids |> tally()
# 485925 credential records
credential_bgs_ids |>
  count(PSI_PEN) %>%
  filter(n > 1) %>%
  tally()
# 21826 have more than one credential
credential_bgs_ids |>
  count(ID) %>%
  filter(n > 1) %>%
  tally()
# NO duplication, ID is the row number, number of credentials

# ---- GRAD Credentials: Credential_Non_Dup_GRAD_IDs ----
# Create a table with only GRAD credentials and finalized CIP codes (133844 rows in 2023)
# GRAD credentials are finalized immediately with STP CIP codes as the final CIP.
# No further matching to survey outcomes is needed (no BGS survey data exists for GRAD).
# These records bypass the matching logic and proceed directly to supply modeling.
# ?never get used?
credential_grad_ids <- stp_cip_ids %>%
  filter(OUTCOMES_CRED == "GRAD") %>%
  select(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED,
    # Immediately finalize GRAD CIPs to STP values (no BGS survey matching available)
    FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME
  ) %>%
  # Materialize as persistent table for use in later supply modeling steps
  {
    if (
      dbExistsTable(
        con,
        Id(schema = my_schema, table = "Credential_Non_Dup_GRAD_IDs")
      )
    ) {
      dbRemoveTable(
        con,
        Id(schema = my_schema, table = "Credential_Non_Dup_GRAD_IDs")
      )
    }
    .
  } %>%
  compute(
    name = Id(schema = my_schema, table = "Credential_Non_Dup_GRAD_IDs"),
    temporary = FALSE
  )

credential_grad_ids |> tally() # verify count matches expected from documentation
# not used in the

## check no blanks for STP_CIP_CODE_4
{
  ## total rows in BGS data
  tbl(con, "Credential_Non_Dup_BGS_IDs") %>% tally()

  ## row with empty CIP4: zero
  chk <- tbl(con, "Credential_Non_Dup_BGS_IDs") %>%
    filter(is.na(STP_CIP_CODE_4)) %>%
    distinct(PSI_CREDENTIAL_CIP, STP_CIP_CODE_4, STP_CIP_CODE_4_NAME) %>%
    collect()

  ## total rows in GRAD data
  tbl(con, "Credential_Non_Dup_GRAD_IDs") %>% tally()
  ## row with empty CIP4: zero
  chk <- tbl(con, "Credential_Non_Dup_GRAD_IDs") %>%
    filter(is.na(FINAL_CIP_CODE_4)) %>%
    distinct(PSI_CREDENTIAL_CIP, FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME) %>%
    collect()

  rm(chk)
}

## check Credential_Non_Dup_BGS_IDs for (Unspecified) - when Credential_Non_Dup loaded NULLs changed to (Unspecified)
{
  chk <- tbl(con, "Credential_Non_Dup_BGS_IDs") %>%
    filter(
      PSI_CODE == "(Unspecified)" |
        PSI_PROGRAM_CODE == "(Unspecified)" |
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION == "(Unspecified)" |
        PSI_CREDENTIAL_CIP == "(Unspecified)" |
        PSI_AWARD_SCHOOL_YEAR == "(Unspecified)" |
        OUTCOMES_CRED == "(Unspecified)"
    ) %>%
    collect()

  ## which columns have "(Unspecified)"
  map(chk, ~ sum(str_detect(.x, "(Unspecified)")))

  ## 2023: only PSI_PROGRAM_CODE had (Unspecified) - replace with NULLs, which is already done.
  # dbGetQuery(
  #   con,
  #   "
  #            Update Credential_Non_Dup_BGS_IDs
  #            SET PSI_PROGRAM_CODE = NULL
  #            WHERE PSI_PROGRAM_CODE = '(Unspecified)'"
  # )

  tbl(con, "Credential_Non_Dup_BGS_IDs") %>%
    filter(PSI_PROGRAM_CODE == "(Unspecified)") %>%
    tally()

  tbl(con, "Credential_Non_Dup_BGS_IDs") %>%
    filter(is.na(PSI_PROGRAM_CODE)) %>%
    tally()
  # still 364449 rows with null PSI_PROGRAM_CODE which are converted from '(Unspecified)'?

  rm(chk)
}

## Part 3: Build Case-level XWALK ----
## Created tables: BGS_Matching_STP_Credential_PEN
##                 BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly
##
##nWHY: BGS and STP use different CIP coding systems. By matching records and applying business rules,
##      we can decide which CIP source is most appropriate for supply modeling. High-confidence matches
##      (where institution, year, and CIP all align) require no further review.
## HOW: 1) Inner join BGS outcomes (T_BGS_Data_Final_for_OutcomesMatching) to STP credentials
##         (Credential_Non_Dup_BGS_IDs) on PEN
##      2) Calculate match flags for: institution code, award year, 4-digit CIP, 2-digit CIP
##      3) Apply institution alias mapping (e.g., CAPU=CAP, UBCO=UBCV=UBC) to handle naming differences
##      4) Create compound flags: MATCH_ALL_3_CIP4_FLAG requires all three (inst, year, CIP4) to match
##      5) Initialize FINAL_CIP columns based on high-confidence matches; remaining records require
##         manual review or secondary matching logic in Parts 3B/3C
##
## Note: This join is many-to-many on PEN - a single BGS STQU_ID and STP Credential ID may appear
##       multiple times if a student has multiple credentials or survey records in the matching periods.

### Part 3A: Initial XWALK ----

# Verify PSI_PEN column exists in STP credentials table (added in Part 2)
colnames(tbl(con, "Credential_Non_Dup_BGS_IDs"))

## Create BGS_Matching_STP_Credential_PEN by performing inner join on PEN
## This table combines BGS survey outcomes with matched STP credentials.
## Each row represents a potential match between a BGS survey record and a STP credential.
## Many-to-many relationships are expected (e.g., student with multiple credentials or survey years).

# id should be unique for updates to be reliable.

t_bgs_final %>%
  count(PEN) %>%
  filter(n > 1) %>%
  tally()

credential_bgs_ids %>%
  count(PSI_PEN) %>%
  filter(n > 1) %>%
  tally()


# 1. Match BGS and STP on PEN (Personal Education Number)
bgs_matching <- t_bgs_final %>%
  # Filter for valid PEN values (exclude blank, missing, or zero values)
  filter(PEN != "", !is.na(PEN), PEN != "0") %>%
  # Inner join: only keep records where PEN exists in both tables
  inner_join(
    credential_bgs_ids,
    by = c("PEN" = "PSI_PEN")
  ) %>%
  # Select and rename columns for clarity in downstream processing
  select(
    STQU_ID, # BGS survey record ID
    ID, # STP credential record ID
    PEN, # Personal Education Number (linking key)
    OUTCOMES_CRED, # Credential type (should be "BGS" for this table)
    INSTITUTION_CODE, # BGS institution code
    PSI_CODE, # STP institution code
    YEAR, # BGS survey year
    PSI_AWARD_SCHOOL_YEAR, # STP credential award year
    BGS_FINAL_CIP_CODE_4 = CIP_4DIGIT_NO_PERIOD, # BGS 4-digit CIP (no periods)
    BGS_FINAL_CIP_CODE_4_NAME = CIP4DIG_NAME, # BGS 4-digit CIP description
    STP_FINAL_CIP_CODE_4 = STP_CIP_CODE_4, # STP normalized 4-digit CIP
    STP_FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME, # STP 4-digit CIP description
    BGS_FINAL_CIP_CODE_2 = CIP2DIG, # BGS 2-digit CIP
    BGS_FINAL_CIP_CODE_2_NAME = CIP2DIG_NAME, # BGS 2-digit CIP description
    STP_FINAL_CIP_CODE_2 = STP_CIP_CODE_2, # STP normalized 2-digit CIP
    STP_FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME, # STP 2-digit CIP description
    BGS_PROGRAM_CODE = CPC, # BGS program code (College Program Code)
    BGS_PROGRAM_DESC = PROGRAM, # BGS program description
    STP_PROGRAM_CODE = PSI_PROGRAM_CODE, # STP program code
    STP_PROGRAM_DESC = PSI_CREDENTIAL_PROGRAM_DESCRIPTION # STP program description
  )

# id should be unique for updates to be reliable.

bgs_matching %>%
  count(ID) %>%
  filter(n > 1) %>%
  tally()

bgs_matching %>%
  count(PEN) %>%
  filter(n > 1) %>%
  tally()

bgs_matching %>%
  count(STQU_ID) %>%
  filter(n > 1) %>%
  tally()

bgs_matching %>%
  count(STQU_ID, ID) %>%
  filter(n > 1) %>%
  tally()

bgs_matching |>
  filter(ID == "849715")
# two rows with different BGS CIPs but the same STP CIPs

# bgs_matching |>
#   group_by(ID) |>
#   mutate(n = n()) |>
#   filter(n > 1) |>
#   glimpse()
#

# STQU_ID, ID should be unique

## Validate: Check row count matches expected from documentation
{
  # Expected count: inner join of BGS records with valid PEN to STP BGS credentials
  tbl(con, "T_BGS_Data_Final_for_OutcomesMatching") %>%
    select(STQU_ID, PEN) %>%
    filter(!is.na(PEN) & PEN != "" & PEN != "0") %>%
    inner_join(
      tbl(con, "Credential_Non_Dup_BGS_IDs") %>% select(ID, PSI_PEN),
      by = c("PEN" = "PSI_PEN")
    ) %>%
    tally() # Expected: 133,952 (2023 data)

  bgs_matching %>% tally() # Verify actual matches expected count
}

### Part 3B: Auto matching using flags ----
### Apply business logic to flag matches on institution, award year, and CIP codes

## Add flag columns to track matching criteria
## These flags will be used to determine which CIP source (BGS or STP) is most reliable.

# 2. Add Match Flags
# Define Institution Match Logic:
# Some institutions have different codes in BGS vs STP. This logic handles known aliases.
# For example: CAPU (BGS) = CAP (STP), UBCO/UBCV (BGS) = UBC (STP)
bgs_matching_flagged <- bgs_matching %>%
  mutate(
    # MATCH_INST: Flag records where BGS and STP institution codes align or match known aliases
    MATCH_INST = case_when(
      PSI_CODE == INSTITUTION_CODE ~ "Yes",
      PSI_CODE == "CAPU" & INSTITUTION_CODE == "CAP" ~ "Yes",
      PSI_CODE == "CAP" & INSTITUTION_CODE == "CAPU" ~ "Yes",
      PSI_CODE == "DOUG" & INSTITUTION_CODE == "DGL" ~ "Yes",
      PSI_CODE == "UCC" & INSTITUTION_CODE == "TRU" ~ "Yes",
      PSI_CODE %in%
        c("ECIAD", "ECU") &
        INSTITUTION_CODE %in% c("ECU", "ECUAD", "ECIAD") ~
        "Yes",
      PSI_CODE %in% c("KWAN", "KPU") & INSTITUTION_CODE %in% c("KPU", "KWN") ~
        "Yes",
      PSI_CODE %in% c("MALA", "MAL") & INSTITUTION_CODE %in% c("VIU", "MAL") ~
        "Yes",
      PSI_CODE %in%
        c("OUC", "OKAN") &
        INSTITUTION_CODE %in% c("OKAN", "OKN", "OUC") ~
        "Yes",
      PSI_CODE == "OLA" & INSTITUTION_CODE == "TRUOL" ~ "Yes",
      PSI_CODE %in%
        c("UCFV", "UFV") &
        INSTITUTION_CODE %in% c("UFV", "FVAL", "UCFV") ~
        "Yes",
      PSI_CODE == "UBCO" & INSTITUTION_CODE == "UBC" ~ "Yes",
      PSI_CODE == "UBCV" & INSTITUTION_CODE == "UBC" ~ "Yes",
      TRUE ~ NA_character_
    ),

    # MATCH_AWARD_SCHOOL_YEAR: Flag records where BGS and STP award years align within 2-year lag
    ## BGS cohort is surveyed 2 years after STP credential award, so a 2-year lag is expected.
    ## Example: BGS 2023 survey should match STP 2020/2021 or 2021/2022 award years.
    ## This case_when must be updated annually as new survey years are added. [YSC11.1][AL11.2]
    MATCH_AWARD_SCHOOL_YEAR = case_when(
      (YEAR == 2000 & PSI_AWARD_SCHOOL_YEAR %in% c("1997/1998", "1998/1999")) |
        (YEAR == 2002 &
          PSI_AWARD_SCHOOL_YEAR %in% c("1999/2000", "2000/2001")) |
        (YEAR == 2004 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2001/2002", "2002/2003")) |
        (YEAR == 2006 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2003/2004", "2004/2005")) |
        (YEAR == 2008 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2005/2006", "2006/2007")) |
        (YEAR == 2009 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2006/2007", "2007/2008")) |
        (YEAR == 2010 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2007/2008", "2008/2009")) |
        (YEAR == 2011 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2008/2009", "2009/2010")) |
        (YEAR == 2012 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2009/2010", "2010/2011")) |
        (YEAR == 2013 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2010/2011", "2011/2012")) |
        (YEAR == 2014 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2011/2012", "2012/2013")) |
        (YEAR == 2015 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2012/2013", "2013/2014")) |
        (YEAR == 2016 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2013/2014", "2014/2015")) |
        (YEAR == 2017 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2014/2015", "2015/2016")) |
        (YEAR == 2018 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2015/2016", "2016/2017")) |
        (YEAR == 2019 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2016/2017", "2017/2018")) |
        (YEAR == 2020 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2017/2018", "2018/2019")) |
        (YEAR == 2021 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2018/2019", "2019/2020")) |
        (YEAR == 2022 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2019/2020", "2020/2021")) |
        (YEAR == 2023 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2020/2021", "2021/2022")) ~
        "Yes",
      TRUE ~ NA_character_
    ),

    # Match_CIP_CODE_4: Flag records where BGS and STP 4-digit CIPs are identical
    ## Exact match on specific program classification = high confidence
    Match_CIP_CODE_4 = if_else(
      BGS_FINAL_CIP_CODE_4 == STP_FINAL_CIP_CODE_4,
      "Yes",
      NA_character_
    ),

    # Match_CIP_CODE_2: Flag records where BGS and STP 2-digit CIPs are identical
    ## Match on broader program category (less specific than 4-digit)
    ## Used as fallback when 4-digit codes differ but broad categories align
    Match_CIP_CODE_2 = if_else(
      BGS_FINAL_CIP_CODE_2 == STP_FINAL_CIP_CODE_2,
      "Yes",
      NA_character_
    )
  ) %>%
  ## Create compound flags combining multiple match criteria
  ## These determine the "confidence level" of each match.
  mutate(
    # MATCH_ALL_3_CIP4_FLAG: Highest confidence match
    ## Requires: institution match AND award year match AND 4-digit CIP match
    ## These records need no further review - use BGS CIP (which equals STP CIP for these matches)
    MATCH_ALL_3_CIP4_FLAG = if_else(
      Match_CIP_CODE_4 == "Yes" &
        MATCH_AWARD_SCHOOL_YEAR == "Yes" &
        MATCH_INST == "Yes",
      "Yes",
      NA_character_
    ),

    # MATCH_ALL_3_CIP2_FLAG: Medium confidence match
    ## Requires: institution match AND award year match AND 2-digit CIP match
    ## These records matched on broader program category but 4-digit codes differ
    ## Require manual review or secondary logic to decide which CIP source to use (Part 3B extended)
    MATCH_ALL_3_CIP2_FLAG = if_else(
      Match_CIP_CODE_2 == "Yes" &
        MATCH_AWARD_SCHOOL_YEAR == "Yes" &
        MATCH_INST == "Yes",
      "Yes",
      NA_character_
    )
  ) %>%
  ## Initialize Final CIP columns based on high-confidence matches
  ## Records not flagged here will be processed in Parts 3B extended/3C

  # Set FINAL_CIP values for highest-confidence matches (MATCH_ALL_3_CIP4_FLAG = "Yes")
  ## For these records, BGS CIP = STP CIP so use BGS as final (no CIP change needed)
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      MATCH_ALL_3_CIP4_FLAG == "Yes",
      BGS_FINAL_CIP_CODE_4,
      NA_character_
    ),

    FINAL_CIP_CODE_2 = if_else(
      MATCH_ALL_3_CIP4_FLAG == "Yes", # Align with 4-digit logic for consistency
      BGS_FINAL_CIP_CODE_2,
      NA_character_
    ),

    # Track which CIP source was selected for this record
    ## "Yes" = BGS CIP was selected, "No" = STP CIP will be selected (in Parts 3B/3C)
    USE_BGS_CIP = if_else(MATCH_ALL_3_CIP4_FLAG == "Yes", "Yes", NA_character_),

    # Flag records that have been finalized with high confidence (no manual review needed)
    FINAL_CONSIDER_A_MATCH = if_else(
      MATCH_ALL_3_CIP4_FLAG == "Yes",
      "Yes",
      NA_character_
    ),

    # Placeholder for records finalized through manual review in Part 3C
    # The reason: dbplyr translates NA_character_ → SQL NULL (no type). SQL Server then defaults untyped NULL to int. Using sql("CAST(NULL AS VARCHAR(255))") forces the database to treat it as a character column.
    FINAL_CIP_CODE_4_NAME = sql("CAST(NULL AS VARCHAR(255))"),
    FINAL_CIP_CODE_2_NAME = sql("CAST(NULL AS VARCHAR(255))"),
    FINAL_CIP_CLUSTER_CODE = sql("CAST(NULL AS VARCHAR(255))"),
    FINAL_CIP_CLUSTER_NAME = sql("CAST(NULL AS VARCHAR(255))"),
    FINAL_PROBABLE_MATCH = sql("CAST(NULL AS VARCHAR(255))")
  )

bgs_matching_flagged |> tally() # Verify row count: 133,952 (2023)
bgs_matching_flagged |> glimpse() # Review structure


## Materialize as persistent SQL table for use in Parts 3B extended/3C
{
  if (
    dbExistsTable(
      con,
      Id(schema = my_schema, table = "BGS_Matching_STP_Credential_PEN")
    )
  ) {
    dbRemoveTable(
      con,
      Id(schema = my_schema, table = "BGS_Matching_STP_Credential_PEN")
    )
  }
}

bgs_matching_flagged <- bgs_matching_flagged |>
  compute(
    name = Id(schema = my_schema, table = "BGS_Matching_STP_Credential_PEN"),
    temporary = FALSE
  )


bgs_matching_flagged |> tally() # Verify: 133,952 (2023)
bgs_matching_flagged |> glimpse() # Review structure

# id should be unique for updates to be reliable.

bgs_matching_flagged %>%
  count(ID) %>%
  filter(n > 1) %>%
  tally()


## Validation: Check for any new institution codes that need mapping
## If you see unmatched PSI_CODEs/INSTITUTION_CODE pairs, add them to the MATCH_INST case_when above
{
  # Find all institution codes that successfully matched
  table <- bgs_matching_flagged %>%
    select(PSI_CODE, INSTITUTION_CODE, MATCH_INST)

  # Get codes that have at least one match (these are working)
  codes <- table %>%
    filter(!is.na(MATCH_INST)) %>%
    distinct(PSI_CODE, INSTITUTION_CODE) %>%
    collect()

  # Show any PSI codes without any successful matches (potential new aliases needed)
  table %>%
    filter(is.na(MATCH_INST) & !PSI_CODE %in% codes$PSI_CODE) %>%
    count(PSI_CODE, INSTITUTION_CODE) %>%
    arrange(PSI_CODE) %>%
    collect()

  rm(table, codes)
}

## Summary statistics: Review match flag distributions
## These counts help validate the matching logic and identify if adjustments are needed
{
  # Institution match rates
  bgs_matching_flagged %>%
    group_by(MATCH_INST) %>%
    tally()

  # Award year match rates (should be high due to structured logic)
  bgs_matching_flagged %>%
    group_by(MATCH_AWARD_SCHOOL_YEAR) %>%
    tally()

  # 4-digit CIP match rates (0 = all codes differ, 1 = some match)
  bgs_matching_flagged %>%
    group_by(MATCH_ALL_3_CIP4_FLAG) %>%
    tally()

  # 2-digit CIP match rates (broader category matches)
  bgs_matching_flagged %>%
    group_by(MATCH_ALL_3_CIP2_FLAG) %>%
    tally()

  ## Detailed breakdown: 4-digit CIP matches by institution
  ## Shows which institutions have high/low match rates (potential problem areas)
  table <- bgs_matching_flagged %>%
    filter(MATCH_INST == "Yes" & MATCH_AWARD_SCHOOL_YEAR == "Yes") %>%
    group_by(INSTITUTION_CODE)

  # Summary: matched vs unmatched record counts and match percentage by institution
  # Institutions with low match percentages may indicate CIP coding alignment issues
  chk <- table %>%
    # Count records that matched on all three criteria (high confidence)
    filter(!is.na(MATCH_ALL_3_CIP4_FLAG)) %>%
    tally() %>%
    # Count records that matched on institution/year but NOT 4-digit CIP (need manual review)
    full_join(
      table %>%
        filter(is.na(MATCH_ALL_3_CIP4_FLAG)) %>%
        tally(),
      by = "INSTITUTION_CODE",
      suffix = c("_matched", "_unmatched")
    ) %>%
    # Total record count per institution
    full_join(
      table %>% tally(),
      by = "INSTITUTION_CODE"
    ) %>%
    # Calculate percentage of records requiring further review
    mutate(perc_unmatched = n_unmatched * 100 / n) %>%
    collect() %>%
    # Sort by unmatched percentage to identify institutions needing attention
    arrange(desc(perc_unmatched))

  rm(table, chk)
}
# many unmatched rows

# ---- Part 3B Extended: Apply Decision Logic for 2-Digit CIP Matches ----
# WHAT: For records that matched on institution and year but differ on 4-digit CIP codes,
#       apply a multi-step decision tree to determine whether BGS or STP CIP codes are more
#       appropriate. This section handles the ~1,500 program combinations that need manual review.
# WHY: Records matching only on 2-digit CIP (broader program category) indicate the student's
#      program was recorded differently between BGS survey and STP credentials. We need to pick
#      the most reliable CIP source for supply modeling.
# HOW: 1) Identify "general programs" - if one source uses a generic code, prefer the other's
#         more specific code
#      2) Cross-reference to 4-digit exact matches - if a program appears in exact matches,
#         that source's CIP is likely more reliable
#      3) Apply custom business logic for known program pairs
#      4) Default remaining cases to STP CIP (consistent with historical approach)

# ---- Step 1: Prepare 2-digit CIP match candidates for algorithmic review ----
# Aggregate all 2-digit CIP matches (institution + year + 2D CIP match, but no 4D match)
# into unique program combinations for systematic decision-making.
# This reduces 133,952 individual records to ~1,600 unique program decision points.

## t1: Aggregate 2-digit CIP matches for program-level decision making
## Filters to records where institution + year + 2D CIP match, but 4D CIP differs
## Groups by all relevant program and CIP identifiers to create unique decision points
## This reduces 133,952 individual records to ~1,600 unique program combinations
## that require algorithmic or manual review to determine which CIP source to use.
## The resulting table serves as reference data for Steps 2A/2B cross-validation logic.
t1 <- bgs_matching_flagged |>
  rename_with(toupper) |>
  group_by(
    INSTITUTION_CODE,
    PSI_CODE,
    YEAR,
    PSI_AWARD_SCHOOL_YEAR,
    BGS_PROGRAM_CODE,
    STP_PROGRAM_CODE,
    BGS_PROGRAM_DESC,
    STP_PROGRAM_DESC,
    BGS_FINAL_CIP_CODE_4,
    BGS_FINAL_CIP_CODE_4_NAME,
    STP_FINAL_CIP_CODE_4,
    STP_FINAL_CIP_CODE_4_NAME,
    BGS_FINAL_CIP_CODE_2,
    BGS_FINAL_CIP_CODE_2_NAME,
    STP_FINAL_CIP_CODE_2,
    STP_FINAL_CIP_CODE_2_NAME,
    MATCH_INST,
    MATCH_AWARD_SCHOOL_YEAR,
    MATCH_CIP_CODE_4,
    MATCH_ALL_3_CIP4_FLAG,
    MATCH_CIP_CODE_2,
    MATCH_ALL_3_CIP2_FLAG
  ) |>
  summarise(
    Expr1 = n(),
    .groups = "drop"
  ) |>
  filter(
    is.na(MATCH_ALL_3_CIP4_FLAG), # Exclude 4-digit exact matches (already high-confidence)
    MATCH_ALL_3_CIP2_FLAG == "Yes" # Include only 2-digit CIP matches (broader program category)
  )

t1 |> tally() # Should be ~1,600 unique combinations (varies by year)

## t2: Aggregated view of 2-digit CIP matches for program-level decision making
## Groups individual records by institution, programs, and CIP codes to create
## unique decision points (program pairs where only 2-digit CIPs match)
t2 <- bgs_matching_flagged %>%
  rename_with(toupper) |>
  filter(MATCH_ALL_3_CIP2_FLAG == "Yes" & is.na(MATCH_ALL_3_CIP4_FLAG)) %>%
  group_by(
    INSTITUTION_CODE,
    PSI_CODE,
    YEAR,
    PSI_AWARD_SCHOOL_YEAR,
    BGS_PROGRAM_CODE,
    STP_PROGRAM_CODE,
    BGS_PROGRAM_DESC,
    STP_PROGRAM_DESC,
    BGS_FINAL_CIP_CODE_4,
    BGS_FINAL_CIP_CODE_4_NAME,
    STP_FINAL_CIP_CODE_4,
    STP_FINAL_CIP_CODE_4_NAME,
    MATCH_ALL_3_CIP2_FLAG
  ) %>%
  summarize(Expr1 = n(), .groups = "drop") %>%
  collect()

t2 |> tally() # ~1,600 unique program decision points

# ---- Step 2: Apply multi-step decision tree to assign CIP_TO_USE ----
# This decision tree prioritizes data quality and consistency:
# - Prefer more specific programs over generic programs
# - Leverage 4-digit exact matches as evidence of reliable coding
# - Use custom rules for known program mapping issues
# - Default to STP for remaining cases (historical consistency)

## Decision Step 1: General Program Logic
## Some CIP codes represent "general" programs (e.g., 1101 = General Agriculture).
## These are less specific than their counterparts. If one source uses a general code,
## prefer the more specific code from the other source. This assumes the more specific
## code better describes the student's actual program of study.
## Note: This logic removes many unassigned cases from ~1,600 to ~300 needing further review.
matched_2d_cips <- t2 %>%
  mutate(
    CIP_TO_USE = case_when(
      # BGS uses a general program - defer to STP's more specific code
      BGS_FINAL_CIP_CODE_4 %in%
        c(
          "1101", # General Agriculture
          "1301", # General Engineering
          "1401", # General Engineering-related fields
          "1901", # General Family and Consumer Sciences
          "2301", # General Biological Sciences
          "2401", # General Biophysics
          "2601", # General Health Sciences
          "4001", # General Public Administration
          "4201", # General Social Sciences
          "4501", # General Area/Ethnic/Cultural Studies
          "5001", # General Humanities
          "5201", # General Visual/Performing Arts
          "5501" # General Business/Commerce
        ) ~ "STP",
      # STP uses a general program - defer to BGS's more specific code
      STP_FINAL_CIP_CODE_4 %in%
        c(
          "1101",
          "1301",
          "1401",
          "1901",
          "2301",
          "2401",
          "2601",
          "4001",
          "4201",
          "4501",
          "5001",
          "5201",
          "5501"
        ) ~ "BGS"
    )
  )

matched_2d_cips |> tally() # ~1,600 rows total
count(matched_2d_cips, CIP_TO_USE) # ~1,300 resolved to BGS/STP, ~300 still unassigned (NA)

## Decision Step 2A: Cross-validate with 4-digit exact matches (STP perspective)
## Strategy: If a program combination (institution + STP program code + STP CIP) appears
## in the 4-digit exact match records (t1), it indicates STP's CIP coding was used in
## a high-confidence match elsewhere. Assign these records to use STP CIP for consistency.
## This leverages existing reliable matches: if STP's CIP aligned perfectly in one case,
## it's likely reliable for this program type at this institution.
matched_2d_cips <- matched_2d_cips %>%
  left_join(
    t1 %>%
      distinct(
        INSTITUTION_CODE,
        STP_PROGRAM_CODE,
        STP_PROGRAM_DESC,
        CIP = BGS_FINAL_CIP_CODE_4, # CIP from 4-digit match records
        STP_FINAL_CIP_CODE_4
      ) |>
      collect(),
    by = c(
      "INSTITUTION_CODE",
      "STP_PROGRAM_CODE",
      "STP_PROGRAM_DESC",
      "STP_FINAL_CIP_CODE_4" # join by the STP CIP CODE which can be used for exact matches
    )
  ) %>%
  mutate(
    # Keep existing decision, or assign STP if program found in 4-digit exact matches
    CIP_TO_USE = case_when(
      !is.na(CIP_TO_USE) ~ CIP_TO_USE,
      !is.na(CIP) ~ "STP" # Program found in 4-digit exact matches - STP CIP proved reliable
    )
  ) %>%
  select(-CIP) |>
  distinct()

matched_2d_cips |> tally() # Expanded due to many-to-many relationships from 4-digit matches
count(matched_2d_cips, CIP_TO_USE) # Most cases now assigned; fewer unassigned (NA) cases remain
# no na anymore

## Decision Step 2B: Cross-validate with 4-digit exact matches (BGS perspective)
## Mirror logic to Step 2A from BGS perspective: if a program combination (institution +
## BGS program code + BGS CIP) appears in 4-digit exact match records (t1), it indicates
## BGS's CIP coding was used in a high-confidence match. Assign these to use BGS CIP
## for consistency. This ensures programs matching on 4-digit BGS CIP elsewhere are
## treated the same way here.
## Warning: This join introduces a many-to-many relationship because a single BGS program
## at an institution may have matched to multiple different STP CIPs in 4-digit exact matches.
## This expands row count significantly (from ~1,600 to ~7,500) but correctly represents
## the multiple program combinations that need individual decisions.
matched_2d_cips <- matched_2d_cips %>%
  left_join(
    t1 %>%
      distinct(
        INSTITUTION_CODE,
        BGS_PROGRAM_CODE,
        BGS_PROGRAM_DESC,
        BGS_FINAL_CIP_CODE_4,
        CIP = STP_FINAL_CIP_CODE_4 # CIP from 4-digit match records
      ) |>
      collect(),
    by = c(
      "INSTITUTION_CODE",
      "BGS_PROGRAM_CODE",
      "BGS_PROGRAM_DESC",
      "BGS_FINAL_CIP_CODE_4"
    ),
    relationship = "many-to-many" # Expected: BGS program may have matched to multiple STP CIPs
  ) %>%
  mutate(
    # Keep existing decision, or assign BGS if program found in 4-digit exact matches
    CIP_TO_USE = case_when(
      !is.na(CIP_TO_USE) ~ CIP_TO_USE,
      !is.na(CIP) ~ "BGS" # Program found in 4-digit exact matches - BGS CIP proved reliable
    )
  ) %>%
  select(-CIP) |>
  distinct()

matched_2d_cips |> tally() # ~7,500 rows after many-to-many join expansion before `distinct` call
count(matched_2d_cips, CIP_TO_USE) # All rows should now have CIP_TO_USE assigned (no NA values)
# nothing changes

## Decision Step 3: Custom mappings for known program pair misalignments
## Some program pairs have systematic misalignments due to historical CIP coding
## differences between BGS INFOWARE and STP systems. These custom rules apply
## institutional knowledge to override the algorithmic decisions when appropriate.

# Math/Applied Math distinction: BGS uses 2701 (Math), STP uses 2703 (Applied Math)
# Programs labeled "operations research" or "mash" use 2703 - follow STP
matched_2d_cips <- matched_2d_cips %>%
  mutate(
    CIP_TO_USE = case_when(
      !is.na(CIP_TO_USE) ~ CIP_TO_USE,
      BGS_FINAL_CIP_CODE_4 == "2701" & STP_FINAL_CIP_CODE_4 == "2703" ~ "STP"
    )
  )

count(matched_2d_cips, CIP_TO_USE)

# Bioengineering vs Chemical Engineering: BGS uses 1405, STP uses 1407
# Programs all say "Chemical Engineering" - follow STP
matched_2d_cips <- matched_2d_cips %>%
  mutate(
    CIP_TO_USE = case_when(
      !is.na(CIP_TO_USE) ~ CIP_TO_USE,
      BGS_FINAL_CIP_CODE_4 == "1405" & STP_FINAL_CIP_CODE_4 == "1407" ~ "STP"
    )
  )

count(matched_2d_cips, CIP_TO_USE)

## Decision Step 4: Default to STP for all remaining cases
## Remaining unresolved cases are predominantly double majors where BGS and STP
## recorded the programs in different orders (e.g., BGS: Business+Engineering,
## STP: Engineering+Business). Default to STP to maintain consistency with
## historical supply modeling practices.
matched_2d_cips <- matched_2d_cips %>%
  mutate(
    CIP_TO_USE = case_when(!is.na(CIP_TO_USE) ~ CIP_TO_USE, TRUE ~ "STP")
  )

count(matched_2d_cips, CIP_TO_USE) # All rows should have a CIP_TO_USE assignment now


matched_2d_cips |> tally()
matched_2d_cips |> glimpse()
matched_2d_cips |> str()

# ---- Stage 2-digit CIP decisions into SQL for main table update ----
# WHAT: Materialize the matched_2d_cips decision table into SQL for efficient joining
#       back to the main BGS_Matching_STP_Credential_PEN table (133,952 records).
# WHY: Joining in-memory R dataframes to SQL tables is inefficient at this scale.
#      Staging the decisions as a SQL table enables fast, database-side joins.
# HOW: 1) Select only the join keys and CIP_TO_USE decision
#      2) Copy to SQL temporary table
#      3) Later: join back to main table and apply decisions (Part 3B.2)

join_keys <- c(
  "INSTITUTION_CODE",
  "PSI_CODE",
  "YEAR",
  "PSI_AWARD_SCHOOL_YEAR",
  "BGS_PROGRAM_CODE",
  "STP_PROGRAM_CODE",
  "BGS_PROGRAM_DESC",
  "STP_PROGRAM_DESC",
  "BGS_FINAL_CIP_CODE_4",
  "STP_FINAL_CIP_CODE_4",
  "MATCH_ALL_3_CIP2_FLAG"
)

stage_cols <- c(
  join_keys,
  "CIP_TO_USE"
)

# Copy decision table to SQL for fast joining
copy_to(
  con,
  matched_2d_cips |> select(stage_cols),
  name = "matched_2d_cips",
  temporary = FALSE,
  overwrite = TRUE
)

src_tbl <- tbl(con, "matched_2d_cips")
src_tbl |> glimpse()
src_tbl |> tally()
# 1593

# ---- Apply 2-digit CIP decisions to main matching table ----
# WHAT: Join the 2-digit CIP decisions back to the full BGS_Matching_STP_Credential_PEN
#       table (133,952 records) and populate FINAL_CIP and USE_BGS_CIP columns for all
#       records that were flagged as MATCH_ALL_3_CIP2_FLAG (but not MATCH_ALL_3_CIP4_FLAG).
# WHY: Updates the main matching table with decisions about which CIP source to use,
#      marking these records as "FINAL_CONSIDER_A_MATCH" so they skip manual review.
# HOW: 1) Left join on multi-key match (institution, programs, years, CIPs)
#      2) Populate FINAL_CIP_CODE_4/2 with decision from matched_2d_cips lookup
#      3) Set USE_BGS_CIP based on CIP_TO_USE (Yes/No)
#      4) Flag as FINAL_CONSIDER_A_MATCH = "Yes" (no more review needed)
#      5) Materialize as updated BGS_Matching_STP_Credential_PEN table
# This left_join behaviours differently in dataframe, and in MSSQL server using dbplyr:
# local dplyr join matches NA to NA
# SQL Server join does not match NULL to NULL
# 1. dataframe:
# When you do a normal dplyr::left_join() on data frames, the default is:
#  `na_matches = "na"`
# NA can match NA
# So if both tables have missing values in one or more join columns, R will treat them as equal and join those rows.
# 2. dbplyr
# When dbplyr translates your join to SQL Server:
# NA becomes NULL
# in SQL, NULL = NULL is not true
# so rows with missing values in join keys do not match
# should specifically add: na_matches = "na" in left join

# Important: use na_matches = "na" so SQL join matches NA-to-NA
# the same way as the local R join

refactored_tbl <- bgs_matching_flagged %>%
  left_join(src_tbl, by = join_keys, na_matches = "na")
refactored_tbl |> glimpse()
# src_tbl brings "CIP_TO_USE" column created by auto-matching

refactored_tbl <- refactored_tbl %>%
  mutate(
    # Use coalesce to keep already-assigned FINAL_CIPs (4-digit exact
    # matches), otherwise apply the 2-digit CIP decision from CIP_TO_USE
    FINAL_CIP_CODE_4 = coalesce(
      FINAL_CIP_CODE_4,
      case_when(
        CIP_TO_USE == "BGS" ~ BGS_FINAL_CIP_CODE_2,
        CIP_TO_USE == "STP" ~ STP_FINAL_CIP_CODE_2
      )
    ),
    #  Align 2-digit CIP with 4-digit decision for consistency
    FINAL_CIP_CODE_2 = coalesce(
      FINAL_CIP_CODE_2,
      case_when(
        CIP_TO_USE == "BGS" ~ BGS_FINAL_CIP_CODE_2,
        CIP_TO_USE == "STP" ~ STP_FINAL_CIP_CODE_2
      )
    ),

    # Track which CIP source was selected (Yes = BGS, No = STP)
    USE_BGS_CIP = coalesce(
      USE_BGS_CIP,
      case_when(
        CIP_TO_USE == "BGS" ~ "Yes",
        CIP_TO_USE == "STP" ~ "No"
      )
    ),

    # Mark records as finalized if a CIP decision was made
    FINAL_CONSIDER_A_MATCH = case_when(
      !is.na(
        coalesce(
          USE_BGS_CIP,
          case_when(
            CIP_TO_USE == "BGS" ~ "Yes",
            CIP_TO_USE == "STP" ~ "No"
          )
        )
      ) ~ "Yes",
      TRUE ~ FINAL_CONSIDER_A_MATCH
    )
  )

refactored_tbl |> glimpse()

refactored_tbl %>%
  count(MATCH_ALL_3_CIP4_FLAG, MATCH_ALL_3_CIP2_FLAG, USE_BGS_CIP) %>%
  collect()

# id should be unique for updates to be reliable.

refactored_tbl %>%
  count(ID) %>%
  filter(n > 1) %>%
  tally()

# ---- Materialize updated matching table ----
# Replace the old BGS_Matching_STP_Credential_PEN with this version that includes
# the 2-digit CIP match decisions. This table will now be passed to Part 3C for
# manual review of remaining unmatched records.

# Update reference to point to final table
bgs_matching_tbl <- refactored_tbl


bgs_matching_tbl |> glimpse()
bgs_matching_tbl |> tally()


# ---- Validation: Verify 2-digit CIP decisions were applied correctly ----
# Check distribution of USE_BGS_CIP by match type to ensure the decision logic worked

bgs_matching_tbl %>%
  count(MATCH_ALL_3_CIP4_FLAG, MATCH_ALL_3_CIP2_FLAG, USE_BGS_CIP) %>%
  collect()
# 19262 rows have unknown/na values for 'USE_BGS_CIP'
bgs_matching_tbl %>%
  count(FINAL_CONSIDER_A_MATCH) %>%
  collect()
# 19262 rows have na values for 'FINAL_CONSIDER_A_MATCH'

# id should be unique for updates to be reliable.

bgs_matching_tbl %>%
  count(ID) %>%
  filter(n > 1) %>%
  tally()

# ---- Part 3C: Manual Matching for Institution & Year Matches with Differing CIPs ----
#
# WHAT: Handles records where BGS survey and STP credentials match on institution and award year,
#       but have different 4-digit CIP codes. These records couldn't be auto-matched in Part 3B
#       because the program classifications diverged between the two systems.
# WHY: Some legitimate program matches have different CIP codes due to how BGS (INFOWARE) and
#      STP classify programs. Rather than discard these matches, we need human judgment to decide
#      whether the mismatch represents: (a) the same program coded differently, or (b) genuinely
#      different programs. This manual review ensures we don't lose valid matches.
# HOW: 1) Extract candidates: records with matching institution & year but divergent CIPs
#      2) Aggregate to program level to reduce manual review workload (~100s of combinations vs ~1000s of records)
#      3) Export for manual review: domain experts mark which CIP source (BGS or STP) is more reliable
#      4) Re-import marked decisions and apply to all matching individual records
#      5) Populate FINAL_CIP columns based on manual decisions
# NOTE: In production, steps 3-4 involve CSV export/import workflow with external stakeholder review.
#       This code provides the infrastructure for that manual process.
#

# ---- Part 3C.1: Extract candidates for manual review ----
# Filter to "borderline" matches: institution code matches, award year matches,
# but 4-digit CIPs differ. These are the records requiring human judgment.

# Note: In a real interactive session, you would:
# - Write manual_candidates to CSV
# - Share with subject matter experts for review
# - Experts mark each row: USE_BGS_CIP = "Yes" (use BGS CIP) or "No" (use STP CIP)
# - Re-import the marked CSV

# ---- Part 3C Extended: Aggregate for manual review workflow ----
# Extract institution/year matches with CIP divergence into a program-level view.
# Aggregating from individual records to unique program combinations reduces
# manual review workload from ~1,000s of individual rows to ~100s of program decisions.
#
# WORKFLOW:
# 1. Extract: Pull all institution+year matches with different CIPs
# 2. Aggregate: Group by program identifiers (institution, program code, program names, CIPs)
# 3. Export: Save aggregated unique program combinations to CSV
# 4. Manual Review: Subject matter experts edit CSV, adding USE_BGS_CIP column (Yes/No)
# 5. Re-import: Read marked CSV back into R
# 6. Join: Apply decisions back to all individual records matching those program pairs
#

# ---- Part 3C.3a: Create initial dataset for manual review ----
# Query all institution+year matches with CIP divergence, formatted for export.

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig <- bgs_matching_tbl %>%
  filter(
    MATCH_INST == "Yes",
    MATCH_AWARD_SCHOOL_YEAR == "Yes",
    is.na(FINAL_CONSIDER_A_MATCH)
  ) |>
  select(
    STQU_ID,
    ID,
    PEN,
    INSTITUTION_CODE,
    PSI_CODE,
    YEAR,
    PSI_AWARD_SCHOOL_YEAR,
    MATCH_INST,
    MATCH_AWARD_SCHOOL_YEAR,
    MATCH_ALL_3_CIP4_FLAG,
    MATCH_ALL_3_CIP2_FLAG,
    FINAL_CONSIDER_A_MATCH,
    BGS_FINAL_CIP_CODE_4,
    BGS_FINAL_CIP_CODE_4_NAME,
    STP_FINAL_CIP_CODE_4,
    STP_FINAL_CIP_CODE_4_NAME,
    BGS_FINAL_CIP_CODE_2,
    BGS_FINAL_CIP_CODE_2_NAME,
    STP_FINAL_CIP_CODE_2,
    STP_FINAL_CIP_CODE_2_NAME,
    BGS_PROGRAM_CODE,
    BGS_PROGRAM_DESC,
    STP_PROGRAM_CODE,
    STP_PROGRAM_DESC,
    USE_BGS_CIP
  ) |>
  collect()
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig |> glimpse()
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig |> tally()
# ~ 6000

# Creates: BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly table in SQL
# This is the full row-level data for the manual review set

# ---- Part 3C.3b: Aggregate to program level for manual review ----
# Reduce row-level data (~1,000s of records) to unique program combinations (~100s).
# Each row represents one unique program pair at one institution where CIP codes differ.

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig_group <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>%
  mutate(across(everything(), trimws)) %>% # Remove excess whitespace
  group_by(
    INSTITUTION_CODE,
    PSI_CODE,
    BGS_FINAL_CIP_CODE_4,
    BGS_FINAL_CIP_CODE_4_NAME,
    STP_FINAL_CIP_CODE_4,
    STP_FINAL_CIP_CODE_4_NAME,
    BGS_PROGRAM_CODE,
    BGS_PROGRAM_DESC,
    STP_PROGRAM_CODE,
    STP_PROGRAM_DESC,
    USE_BGS_CIP # Will be NA at this stage (no decisions yet)
  ) %>%
  summarize(Count = n(), .groups = "drop")

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig_group |> glimpse()
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig_group |> tally()
# ~ 1700

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig_group |>
  collect() %>% # Count records per program combination
  write_csv(
    "BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos_orig.csv"
  )
# NEXT STEP (manual):
# - Open CSV in Excel
# - For each row, add USE_BGS_CIP = "Yes" (use BGS CIP) or "No" (use STP CIP)
# - Save as: BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos.csv
# - Return to this script

# ---- Part 3C.3c: Re-import manual decisions ----
# Read back the CSV with manual USE_BGS_CIP decisions from subject matter experts.

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos <- read_csv(
  "BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos.csv"
)

# ---- Part 3C.3d: Broadcast manual decisions to all matching records ----
# Join aggregated program-level decisions back to individual records.
# This propagates the manual decision for a program pair to all student records
# matching that program pair at that institution.

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig |> count(USE_BGS_CIP)
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos |>
  count(USE_BGS_CIP)


BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>%
  mutate(across(everything(), trimws)) %>% # Normalize whitespace from CSV round-trip
  select(-USE_BGS_CIP) %>% # Remove NA values from initial extraction
  left_join(
    # Join manual decisions by all program identifiers to match the aggregation key
    BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos %>%
      mutate(across(everything(), trimws)) %>%
      # select(-BGS_FINAL_CIP_CODE_4_NAME, -STP_FINAL_CIP_CODE_4_NAME) |> # Avoid duplicate columns
      mutate(across(everything(), as.character)),
    by = c(
      "INSTITUTION_CODE",
      "PSI_CODE",
      "BGS_FINAL_CIP_CODE_4",
      "BGS_FINAL_CIP_CODE_4_NAME",
      "STP_FINAL_CIP_CODE_4",
      "STP_FINAL_CIP_CODE_4_NAME",
      "BGS_PROGRAM_CODE",
      "BGS_PROGRAM_DESC",
      "STP_PROGRAM_CODE",
      "STP_PROGRAM_DESC"
      # "INSTITUTION_CODE",
      # "PSI_CODE",
      # "BGS_FINAL_CIP_CODE_4",
      # "STP_FINAL_CIP_CODE_4",
      # "BGS_PROGRAM_CODE",
      # "BGS_PROGRAM_DESC",
      # "STP_PROGRAM_CODE",
      # "STP_PROGRAM_DESC"
    )
  )

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly |> glimpse()
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly |> count(USE_BGS_CIP)
# still 753 nas?

# ---- Part 3C.3e: Validate manual decisions were applied to all records ----
# Check that every record has a USE_BGS_CIP decision (no NAs).
# If NAs remain, it indicates a mismatch in the join keys between tables.

{
  # Count records by USE_BGS_CIP decision
  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
    count(USE_BGS_CIP)

  # If NAs detected, debug by comparing STP_PROGRAM_CODE values in both tables
  if (
    BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
      filter(is.na(USE_BGS_CIP)) %>%
      nrow() >
      0
  ) {
    # Check for mismatches in program codes
    chk1 <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos %>%
      count(STP_PROGRAM_CODE)
    chk2 <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>%
      count(STP_PROGRAM_CODE)

    # Rows in orig but not in manual decisions would cause NAs after join
    anti_join(chk2, chk1, by = "STP_PROGRAM_CODE")

    rm(chk1, chk2)
  }
}

# ---- Part 3C.3f: Populate final CIP columns based on manual decisions ----
# For each record, assign FINAL_CIP values based on the manual decision.
# USE_BGS_CIP = "Yes" → use BGS CIP codes
# USE_BGS_CIP = "No" → use STP CIP codes

## Update BGS_Matching_STP_Credential_PEN with final CIPs chosen manually
{
  ## may want to save a back up copy of BGS_Matching_STP_Credential_PEN before updating it
  ## in case you want to make changes to the manual matching
  if (
    dbExistsTable(
      con,
      name = Id(
        schema = my_schema,
        table = "BGS_Matching_STP_Credential_PEN_bu"
      )
    )
  ) {
    dbRemoveTable(
      con,
      name = Id(
        schema = my_schema,
        table = "BGS_Matching_STP_Credential_PEN_bu"
      )
    )
  }
  dbExecute(
    con,
    glue::glue(
      "SELECT * INTO [{my_schema}].BGS_Matching_STP_Credential_PEN_bu 
      FROM [{my_schema}].BGS_Matching_STP_Credential_PEN"
    )
  )
}


# ---- Part 3C.2: Apply default logic for unreviewed candidates ----
# For any records without explicit manual decision, default to STP CIP.
# This fallback ensures all candidates get a final CIP assignment.
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly |> glimpse()
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly |> tally()


BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
  mutate(
    # Default to "No" (use STP) if manual review didn't provide a decision
    # USE_BGS_CIP = coalesce(USE_BGS_CIP, "No"),

    # Assign final 4-digit CIP based on decision
    FINAL_CIP_CODE_4 = if_else(
      USE_BGS_CIP == "Yes",
      BGS_FINAL_CIP_CODE_4, # Use BGS CIP if marked during manual review
      STP_FINAL_CIP_CODE_4 # Use STP CIP by default or if "No" marked
    ),

    # Align 2-digit CIP with 4-digit decision for consistency
    FINAL_CIP_CODE_2 = if_else(
      USE_BGS_CIP == "Yes",
      BGS_FINAL_CIP_CODE_2,
      STP_FINAL_CIP_CODE_2
    ),
  ) %>%
  # Keep only columns needed to update the main matching table
  select(
    STQU_ID,
    ID,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_2,
    USE_BGS_CIP
  )

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly |>
  glimpse()

# ---- Part 3C.3g: Validate final CIPs are populated for all decisions ----
# Verify that every record now has FINAL_CIP_CODE_4 and FINAL_CIP_CODE_2 values.
# Any remaining NAs indicate missing manual review decisions.

{
  # Count records with blank final CIPs (should be zero)
  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
    filter(is.na(FINAL_CIP_CODE_4)) %>%
    count(USE_BGS_CIP)

  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
    filter(is.na(FINAL_CIP_CODE_2)) %>%
    count(USE_BGS_CIP)
}

# ---- Part 3C.3: Upload manual decisions and update main matching table ----
# Apply the manual decisions back to BGS_Matching_STP_Credential_PEN table,
# replacing any placeholder values with finalized CIP assignments.

if (nrow(BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly) > 0) {
  # Stage manual updates to temporary SQL table for efficient database joining
  copy_to(
    con,
    BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly,
    Id(
      schema = my_schema,
      table = "BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly"
    ),
    temporary = FALSE,
    overwrite = TRUE
  )

  # Join manual decisions back to main matching table

  # Source table
  source_tbl <- tbl(
    con,
    DBI::Id(
      schema = my_schema,
      table = "BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly"
    )
  )
  source_tbl |> glimpse()
  source_tbl |> tally()
  # chekc unique keys
  source_tbl |> count(STQU_ID, ID) |> collect() |> filter(n > 1) |> tally()

  bgs_matching_tbl |> glimpse()
  bgs_matching_tbl |> tally()
  bgs_matching_tbl |>
    count(STQU_ID, ID) |>
    collect() |>
    filter(n > 1) |>
    tally()
  # good so both do not have duplication

  # Build the rows that should update
  ## qry_update_CIP_for_MatchingYearInstOnly_step1 ----
  bgs_matching_updated <- bgs_matching_tbl %>%
    left_join(
      source_tbl %>%
        transmute(
          ID,
          STQU_ID,
          src_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
          src_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
          src_USE_BGS_CIP = USE_BGS_CIP
        ),
      by = c("ID", "STQU_ID"),
      na_matches = "never"
    ) %>%
    mutate(
      needs_update = is.na(FINAL_PROBABLE_MATCH) &
        is.na(FINAL_CIP_CODE_4) &
        is.na(FINAL_CIP_CODE_2) &
        !is.na(src_FINAL_CIP_CODE_4)
    ) %>%
    mutate(
      FINAL_PROBABLE_MATCH = case_when(
        needs_update == T ~ "Yes",
        TRUE ~ FINAL_PROBABLE_MATCH
      ),
      FINAL_CIP_CODE_4 = case_when(
        needs_update == T ~ src_FINAL_CIP_CODE_4,
        TRUE ~ FINAL_CIP_CODE_4
      ),
      FINAL_CIP_CODE_2 = case_when(
        needs_update == T ~ src_FINAL_CIP_CODE_2,
        TRUE ~ FINAL_CIP_CODE_2
      ),
      USE_BGS_CIP = case_when(
        needs_update == T ~ src_USE_BGS_CIP,
        TRUE ~ USE_BGS_CIP
      )
    ) %>%
    select(-needs_update, -starts_with("src_"))

  # bgs_matching_updated |> glimpse()
} else {
  # No manual candidates - use existing matched table
  bgs_matching_updated <- bgs_matching_tbl
}

bgs_matching_updated |> show_query()
bgs_matching_updated |> count(FINAL_PROBABLE_MATCH) |> collect()

bgs_matching_updated |> glimpse()
bgs_matching_updated |> tally()


##

## qry_update_CIP_for_MatchingYearInstOnly_step2 ----
# Update the rest of the records to use the STP CIPs as final if no match was found
bgs_matching_updated <- bgs_matching_updated |>
  mutate(
    # Identify records where all target columns are still NULL
    needs_stp_fallback = is.na(FINAL_CIP_CODE_4) &
      is.na(FINAL_CIP_CODE_4_NAME) &
      is.na(FINAL_CIP_CODE_2) &
      is.na(FINAL_CIP_CODE_2_NAME)
  ) |>
  mutate(
    # # Apply fallback values from STP source columns
    FINAL_CIP_CODE_4 = if_else(
      needs_stp_fallback == TRUE,
      STP_FINAL_CIP_CODE_4,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      needs_stp_fallback == TRUE,
      STP_FINAL_CIP_CODE_4_NAME,
      FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      needs_stp_fallback == TRUE,
      STP_FINAL_CIP_CODE_2,
      FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      needs_stp_fallback == TRUE,
      STP_FINAL_CIP_CODE_2_NAME,
      FINAL_CIP_CODE_2_NAME
    ),
    USE_BGS_CIP = if_else(needs_stp_fallback == TRUE, "No", USE_BGS_CIP)
  ) |>
  select(-needs_stp_fallback)

bgs_matching_updated |> show_query()
bgs_matching_updated |> glimpse()
bgs_matching_updated |> tally()

## check remaining non-matches: compare program descriptions to ensure they really are non-matches
{
  ## get IDs of matches
  ids_exact <- bgs_matching_updated %>%
    filter(FINAL_CONSIDER_A_MATCH == "Yes") %>%
    distinct(ID) %>%
    collect()
  ids_probable <- bgs_matching_updated %>%
    filter(FINAL_PROBABLE_MATCH == "Yes") %>%
    distinct(ID) %>%
    collect()

  ## filter out non-matches for students that have an existing matched program
  ## review non-matches to see if any should be matched - if so, redo Part 3C to this point
  chk <- bgs_matching_updated %>%
    filter(is.na(FINAL_CIP_CODE_4)) %>% ## filter on empty FINAL CIP
    filter(!is.na(MATCH_INST) & !is.na(MATCH_AWARD_SCHOOL_YEAR)) %>% ## remove records that don't match on institution or year
    collect() %>%
    anti_join(ids_exact, by = "ID") %>% ## remove records that already have a match (from flags)
    anti_join(ids_probable, by = "ID") %>% ## remove records that already have a match (from manual)
    group_by(
      INSTITUTION_CODE,
      BGS_FINAL_CIP_CODE_4,
      BGS_FINAL_CIP_CODE_4_NAME,
      STP_FINAL_CIP_CODE_4,
      STP_FINAL_CIP_CODE_4_NAME,
      BGS_PROGRAM_CODE,
      BGS_PROGRAM_DESC,
      STP_PROGRAM_CODE,
      STP_PROGRAM_DESC,
      USE_BGS_CIP
    ) %>%
    summarize(Count = n(), .groups = "drop")

  rm(chk, ids_exact, ids_probable)
}

bgs_matching_updated |> count(FINAL_CONSIDER_A_MATCH) |> collect()
bgs_matching_updated |> count(FINAL_PROBABLE_MATCH) |> collect()
bgs_matching_updated |>
  count(FINAL_CONSIDER_A_MATCH, FINAL_PROBABLE_MATCH) |>
  collect()
# why are there ~10,000 no match yet

# ??
# id should be unique for updates to be reliable.

bgs_matching_updated |> tally()
# 133952 rows

bgs_matching_updated %>%
  count(ID) %>%
  filter(n > 1) %>%
  tally()
# still ~5000 has at least two rows
bgs_matching_updated %>%
  count(STQU_ID) %>%
  filter(n > 1) %>%
  tally()
# ~ 11,000
bgs_matching_updated %>%
  count(STQU_ID, ID) %>%
  filter(n > 1) %>%
  tally()
# zero rows

## check
{
  bgs_matching_updated %>%
    count(
      USE_BGS_CIP,
      FINAL_CIP_CODE_4 == STP_FINAL_CIP_CODE_4,
      FINAL_CIP_CODE_4 == BGS_FINAL_CIP_CODE_4
    )
}
# ? result is confusing

## remove local tables
rm(
  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly,
  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig,
  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos
)

### Part 3D: Fill in Final Columns ----

# ---- Part 3D: Final Fill (CIP Names and Clusters) ----
#
# WHAT: Finalizes CIP codes for all records and enriches with names and cluster assignments.
# WHY: Some records may still have NULL CIP codes after matching. We need to ensure complete coverage
#      and add human-readable descriptions for reporting.
# HOW: 1) Default remaining NULL CIP codes to STP values
#      2) Join to 4-digit CIP names table
#      3) Join to 2-digit CIP names and cluster tables
#      4) Materialize final matching table
## Add in FINAL_CIP_CODE_4_NAME
# dbGetQuery(con, qry_fill_final_CIP4_NAME)

bgs_matching_final <- bgs_matching_updated %>%
  # Add Names
  ## qry_fill_final_CIP4_NAME ----
  ## New: fill in CIP4 NAME by linking to infoware table
  left_join(
    cip_4_tbl %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("FINAL_CIP_CODE_4" = "LCP4_CD")
  ) %>%

  ## qry_fill_final_CIP2_NAME_and_CLUSTER ----
  ## New: fill in CIP2 NAME and cluster code/name from infoware table
  left_join(
    cip_2_tbl %>%
      select(LCP2_CD, LCP2_DIGITS_NAME, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  #  don't understand why now we reset all those columns. If so, why we bother to set values for them in previous steps.
  mutate(
    FINAL_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME,
    FINAL_CIP_CODE_2_NAME = LCP2_DIGITS_NAME,
    # dbGetQuery(con, qry_fill_final_CIP2_NAME_and_CLUSTER)
    ## New: fill in CIP2 NAME and cluster code/name from infoware table
    FINAL_CIP_CLUSTER_CODE = LCP2_LCIPPC_CD,
    FINAL_CIP_CLUSTER_NAME = LCP2_LCIPPC_NAME
  ) %>%
  # # Default remaining cluster info
  # mutate(
  #   FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
  #   FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  # ) %>%
  select(-LCP4_CIP_4DIGITS_NAME, -LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

bgs_matching_final |> glimpse()
bgs_matching_final |> tally()

## check
{
  bgs_matching_updated %>%
    count(
      USE_BGS_CIP,
      FINAL_CIP_CODE_4 == STP_FINAL_CIP_CODE_4,
      FINAL_CIP_CODE_4 == BGS_FINAL_CIP_CODE_4
    )
}
# ?confusing, could be nas

# Materialize the update to the physical database

output_name <- "BGS_Matching_STP_Credential_PEN"
temp_name <- "BGS_Matching_STP_Credential_PEN_temp"

# Compute to temporary table first to ensure success before modifying the original
bgs_matching_final <- bgs_matching_final |>
  compute(
    name = Id(schema = my_schema, table = temp_name),
    temporary = FALSE
  )

if (
  dbExistsTable(
    con,
    name = Id(schema = my_schema, table = output_name)
  )
) {
  dbRemoveTable(
    con,
    name = Id(schema = my_schema, table = output_name)
  )
}

# Now rename temporary table to final name using SQL Server system procedure
dbExecute(
  con,
  glue::glue("EXEC sp_rename '{my_schema}.{temp_name}', '{output_name}'")
)

# Update reference to point to the finalized SQL table
bgs_matching_final <- tbl(
  con,
  Id(schema = my_schema, table = output_name)
)

## check for blanks
{
  bgs_matching_final %>%
    filter(is.na(FINAL_CIP_CLUSTER_CODE)) %>%
    tally()

  bgs_matching_final %>%
    count(FINAL_CONSIDER_A_MATCH, FINAL_PROBABLE_MATCH) |>
    collect()
}
# ? still ~14000 NAs

## Part 4: Update Credential_Non_Dup  ----

# ---- Part 4: Update Credential_Non_Dup_BGS_IDs ----
## Created tables: Credential_Unmatched_CIPS_to_update
## Updated tables: Credential_Non_Dup_BGS_IDs
#
# WHAT: Updates the BGS credential IDs table with matched CIP codes from the PEN matching workflow.
# WHY: The matching results need to be propagated back to the source credential table for
#      downstream cohort building and supply projections.
# HOW: 1) Left join matching results to BGS IDs table
#      2) Use coalesce to prefer matched CIPs, fallback to original STP values
#      3) Copy match flags and metadata
#      4) Fill missing cluster information from CIP lookup tables
#      5) Materialize updated table

### Part 4A: Update with XWALK ----

{
  ## may want to make a backup copy of Credential_Non_Dup_BGS_IDs
  ## in case you want to make changes to the manual matching
  if (
    dbExistsTable(
      con,
      name = Id(schema = my_schema, table = "Credential_Non_Dup_BGS_IDs_bu")
    )
  ) {
    dbRemoveTable(
      con,
      name = Id(schema = my_schema, table = "Credential_Non_Dup_BGS_IDs_bu")
    )
  }
  dbExecute(
    con,
    glue::glue(
      "select * into [{my_schema}].Credential_Non_Dup_BGS_IDs_bu from [{my_schema}].Credential_Non_Dup_BGS_IDs"
    )
  )
}

## Fill in final CIPS with BGS_Matching_STP_Credential_PEN
# dbGetQuery(con, qry_BGS_IDs_Credential_add_columns)
# dbGetQuery(con, qry_update_Credential_Non_Dup_BGS_IDS_CIP_matches_step1) ## fill in final CIP, etc. from BGS_Matching_STP_Credential_PEN where FINAL_CONSIDER_A_MATCH is not empty
# dbGetQuery(con, qry_update_Credential_Non_Dup_BGS_IDS_CIP_matches_step2) ## fill in still empty CIP, etc. from BGS_Matching_STP_Credential_PEN where FINAL_PROBABLE_MATCH is not empty

# id should be unique for updates to be reliable.

bgs_matching_final %>%
  count(ID, STQU_ID) %>%
  filter(n > 1) %>%
  tally()

bgs_matching_final %>%
  count(ID) %>%
  filter(n > 1) %>%
  tally()
# over ~ 5000

bgs_matching_final %>%
  filter(!is.na(FINAL_CONSIDER_A_MATCH) | !is.na(FINAL_PROBABLE_MATCH)) %>%
  select(
    ID,
    MATCH_FINAL_CIP_4 = FINAL_CIP_CODE_4,
    MATCH_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
    MATCH_FINAL_CIP_2 = FINAL_CIP_CODE_2,
    MATCH_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME,
    MATCH_FINAL_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
    MATCH_FINAL_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
    MATCH_USE_BGS = USE_BGS_CIP,
    MATCH_BGS_CIP_4 = BGS_FINAL_CIP_CODE_4,
    MATCH_BGS_CIP_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
    FINAL_CONSIDER_A_MATCH,
    FINAL_PROBABLE_MATCH
  ) %>%
  distinct() |> # only remove 10 rows
  count(ID) %>%
  filter(n > 1) %>%
  tally()
# over ~ 300, left join with ID will create over 300 duplications.

bgs_matching_final %>%
  filter(!is.na(FINAL_CONSIDER_A_MATCH) | !is.na(FINAL_PROBABLE_MATCH)) %>%
  select(
    ID,
    MATCH_FINAL_CIP_4 = FINAL_CIP_CODE_4,
    MATCH_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
    MATCH_FINAL_CIP_2 = FINAL_CIP_CODE_2,
    MATCH_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME,
    MATCH_FINAL_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
    MATCH_FINAL_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
    MATCH_USE_BGS = USE_BGS_CIP,
    MATCH_BGS_CIP_4 = BGS_FINAL_CIP_CODE_4,
    MATCH_BGS_CIP_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
    FINAL_CONSIDER_A_MATCH,
    FINAL_PROBABLE_MATCH
  ) %>%
  distinct() |> # only remove 10 rows
  group_by(ID) %>%
  mutate(n = n()) %>%
  filter(n > 1) %>%
  glimpse()

credential_bgs_updated <- tbl(
  con,
  in_schema(my_schema, "Credential_Non_Dup_BGS_IDs")
)

credential_bgs_updated |> glimpse()

credential_bgs_updated %>%
  count(ID) %>%
  filter(n > 1) %>%
  tally()
# No duplication

credential_bgs_updated <- credential_bgs_updated %>%
  left_join(
    bgs_matching_final %>%
      filter(!is.na(FINAL_CONSIDER_A_MATCH) | !is.na(FINAL_PROBABLE_MATCH)) %>%
      select(
        ID,
        MATCH_FINAL_CIP_4 = FINAL_CIP_CODE_4,
        MATCH_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
        MATCH_FINAL_CIP_2 = FINAL_CIP_CODE_2,
        MATCH_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME,
        MATCH_FINAL_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
        MATCH_FINAL_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
        MATCH_USE_BGS = USE_BGS_CIP,
        MATCH_BGS_CIP_4 = BGS_FINAL_CIP_CODE_4,
        MATCH_BGS_CIP_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
        FINAL_CONSIDER_A_MATCH,
        FINAL_PROBABLE_MATCH
      ),
    by = "ID"
  ) %>%
  # equivalent: all new columns in credential bgs ids table are NAs.
  # OUTCOMES_CIP_CODE_4
  # OUTCOMES_CIP_CODE_4_NAME
  # FINAL_CONSIDER_A_MATCH
  # FINAL_PROBABLE_MATCH
  # USE_BGS_CIP
  # FINAL_CIP_CODE_4
  # FINAL_CIP_CODE_4_NAME
  # FINAL_CIP_CODE_2
  # FINAL_CIP_CODE_2_NAME
  # FINAL_CIP_CLUSTER_CODE
  # FINAL_CIP_CLUSTER_NAME

  ## Fill in remaining final CIPS with STP CIP from Credential_Non_Dup_BGS_IDS
  #  plan
  ## qry_update_Credential_Non_Dup_BGS_IDS_CIP_matches_step1 ----
  # fill in FINAL CIP columns from BGS_Matching_STP_Credential_PEN where FINAL_CONSIDER_A_MATCH is yes (i.e., where programs were “auto” matched with the flags)
  ## qry_update_Credential_Non_Dup_BGS_IDS_CIP_matches_step2 ----
  # fill in FINAL CIP columns from BGS_Matching_STP_Credential_PEN where FINAL_PROBABLE_MATCH is yes (i.e., where programs were manually matched). Note these CIPs are updated after the auto matches CIPs so that the auto matches take priority in situations where there are duplicate IDs in BGS_Matching_STP_Credential_PEN.

  ## qry_update_remaining_BGS_CIPs_in_Cred_Non_Dup_BGS_IDS_step1 ----
  # use STP CIPs as the FINAL CIPs where programs do not have a match
  mutate(
    FINAL_CIP_CODE_4 = coalesce(MATCH_FINAL_CIP_4, STP_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = coalesce(
      MATCH_FINAL_CIP_4_NAME,
      STP_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = coalesce(MATCH_FINAL_CIP_2, STP_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(
      MATCH_FINAL_CIP_2_NAME,
      STP_CIP_CODE_2_NAME
    ),
    # •	qry_update_remaining_BGS_CIPs_in_ T_BGS_Data_step2 to fill in the CIP_CLUSTER info for the remaining CIPS that did not have a match
    FINAL_CIP_CLUSTER_CODE = MATCH_FINAL_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME = MATCH_FINAL_CLUSTER_NAME,
    USE_BGS_CIP = coalesce(MATCH_USE_BGS, "No because no match"),
    OUTCOMES_CIP_CODE_4 = MATCH_BGS_CIP_4,
    OUTCOMES_CIP_CODE_4_NAME = MATCH_BGS_CIP_4_NAME
  )

## qry_update_remaining_BGS_CIPs_in_Cred_Non_Dup_BGS_IDS_step2 ----
# Fill missing cluster info for unmatched
credential_bgs_updated <- credential_bgs_updated |>
  left_join(
    cip_2_tbl %>% select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME) %>%
  # Default remaining to STP
  mutate(
    FINAL_CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, STP_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = coalesce(
      FINAL_CIP_CODE_4_NAME,
      STP_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, STP_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(
      FINAL_CIP_CODE_2_NAME,
      STP_CIP_CODE_2_NAME
    ),
    USE_BGS_CIP = coalesce(USE_BGS_CIP, "No")
  )

credential_bgs_updated |>
  group_by(ID) %>%
  mutate(n = n()) %>%
  filter(n > 1) %>%
  glimpse()


### Part 4B: Update Unmatched CIPs ----

## Create a list of programs that matched to outcomes data and use BGS CIPs instead of STP
# Credential_Matched_CIPS_using_BGS <- dbGetQuery(
#   con,
#   qry_List_STP_Credential_Non_Dup_Using_BGS_CIPS
# )
## find programs using BGS

Credential_Matched_CIPS_using_BGS <- credential_bgs_updated %>%
  filter(USE_BGS_CIP == "Yes") %>%
  group_by(
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    OUTCOMES_CIP_CODE_4,
    OUTCOMES_CIP_CODE_4_NAME,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME,
    STP_CIP_CODE_2,
    STP_CIP_CODE_2_NAME,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME,
    FINAL_CONSIDER_A_MATCH,
    FINAL_PROBABLE_MATCH,
    USE_BGS_CIP
  ) %>%
  summarise(
    Expr1 = n(),
    .groups = "drop"
  )


## Create a list of programs that did not match to outcomes data
# Credential_Unmatched_CIPS <- dbGetQuery(
#   con,
#   qry_List_STP_Credential_Non_Dup_Umatched
# )
## find unmatched programs

Credential_Unmatched_CIPS <- credential_bgs_updated %>%
  filter(USE_BGS_CIP == "No because no match") %>%
  group_by(
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    OUTCOMES_CIP_CODE_4,
    OUTCOMES_CIP_CODE_4_NAME,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME,
    STP_CIP_CODE_2,
    STP_CIP_CODE_2_NAME,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME,
    FINAL_CONSIDER_A_MATCH,
    FINAL_PROBABLE_MATCH,
    USE_BGS_CIP
  ) %>%
  summarise(
    Expr1 = n(),
    .groups = "drop"
  )

## Combine the lists to find any unmatched programs that were matched to outcomes for different records
## Filter where the BGS and STP CIPs differ
Credential_Unmatched_CIPS_to_review <- Credential_Unmatched_CIPS %>%
  select(-OUTCOMES_CIP_CODE_4, -OUTCOMES_CIP_CODE_4_NAME) %>%
  left_join(
    Credential_Matched_CIPS_using_BGS %>%
      distinct(
        PSI_CODE,
        PSI_PROGRAM_CODE,
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        STP_CIP_CODE_4,
        OUTCOMES_CIP_CODE_4,
        OUTCOMES_CIP_CODE_4_NAME
      ),
    by = c(
      "PSI_CODE",
      "PSI_PROGRAM_CODE",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
      "STP_CIP_CODE_4"
    )
  ) %>%
  mutate(
    Unmatched_But_in_BGS_Program = case_when(
      !is.na(OUTCOMES_CIP_CODE_4) ~ 'Yes'
    ),
    BGS_CIP_is_Different = case_when(
      OUTCOMES_CIP_CODE_4 != STP_CIP_CODE_4 ~ 'Yes'
    )
  ) %>%
  group_by(
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    STP_CIP_CODE_4
  ) %>%
  filter(
    Unmatched_But_in_BGS_Program == "Yes" & BGS_CIP_is_Different == "Yes"
  ) %>%
  select(
    -PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    everything(),
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION
  ) %>%
  arrange(FINAL_CIP_CODE_4)

Credential_Unmatched_CIPS_to_review |> glimpse()
Credential_Unmatched_CIPS_to_review |> tally()


## review the outcomes credentials matched to the unmatched programs
## filter out programs with more than one match
## if any should be updated - update the custom query below
chk <- Credential_Unmatched_CIPS_to_review %>%
  group_by(
    PSI_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME
  ) %>%
  summarize(
    OUTCOMES_CIP_NAME = str_flatten(OUTCOMES_CIP_CODE_4_NAME, collapse = "\n "),
    OUTCOMES_CIP_CODE = str_flatten(OUTCOMES_CIP_CODE_4, collapse = "\n "),
    count = n()
  ) %>%
  filter(count == 1)


## make a table with PROGRAM_DESCRIPTIONs decided to update
Credential_Unmatched_CIPS_to_update <- tibble::tribble(
  ~PSI_CREDENTIAL_PROGRAM_DESCRIPTION                                         , ~FINAL_CIP_CODE_4 , ~FINAL_CIP_CODE_2 ,
  "Bachelor Of Applied Science In Mechatronic Systems Engineering"            ,              1442 ,                14 ,
  "Bachelor Of Athletic And Exercise Therapy"                                 ,              5123 ,                51 ,
  "Bachelor Of Fine Arts In Dance"                                            ,              5003 ,                50 ,
  "Bachelor Of Fine Arts In Film"                                             ,              5006 ,                50 ,
  "Bachelor Of Fine Arts In Music - Composition"                              ,              5009 ,                50 ,
  "Bachelor Of Fine Arts In Music - Electroacoustic"                          ,              5009 ,                50 ,
  "Bachelor Of Fine Arts In Theatre - Performance"                            ,              5005 ,                50 ,
  "Bachelor Of Fine Arts In Theatre - Production And Design"                  ,              5005 ,                50 ,
  "Bachelor Of Science In Geographic Information Science"                     ,              4507 ,                45 ,
  "Bachelor Of Social Work In Indigenous Child Welfare"                       ,              4407 ,                44 ,
  "Bachelor Of Social Work In Indigenous Social Work"                         ,              4407 ,                44 ,
  "Bachelor Of Child & Youth Care In Child & Youth Care"                      ,              1907 ,                19 ,
  "Bachelor Of Child & Youth Care In Child & Youth Care - Child Life Stream"  ,              1907 ,                19 ,
  "Bachelor Of Child & Youth Care In Child & Youth Care - Early Years Stream" ,              1907 ,                19 ,
  "Bachelor Of Child & Youth Care In Child & Youth Care - Child Protection"   ,              1907 ,                19 ,
  "Bachelor Of Child & Youth Care In Child & Youth Care - Indigenous Stream"  ,              1907 ,                19
)

## write to SQL
dbWriteTable(
  con,
  "Credential_Unmatched_CIPS_to_update",
  Credential_Unmatched_CIPS_to_update
)

Credential_Unmatched_CIPS_to_update <- tbl(
  con,
  in_schema(my_schema, "Credential_Unmatched_CIPS_to_update")
)

## update Credential_Non_Dup_BGS_IDs so unmatched programs use linked BGS CIP instead
# dbGetQuery(con, qry_update_Credential_Non_DUP_BGS_IDs_unmatched)

credential_bgs_updated <- credential_bgs_updated %>%
  left_join(
    Credential_Unmatched_CIPS_to_update %>%
      select(
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        FINAL_CIP_CODE_4,
        FINAL_CIP_CODE_2
      ) %>%
      rename(
        upd_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
        upd_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2
      ),
    by = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CONSIDER_A_MATCH) &
        is.na(FINAL_PROBABLE_MATCH) &
        !is.na(upd_FINAL_CIP_CODE_4),
      upd_FINAL_CIP_CODE_4,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CONSIDER_A_MATCH) &
        is.na(FINAL_PROBABLE_MATCH) &
        !is.na(upd_FINAL_CIP_CODE_4),
      NA_character_,
      FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      is.na(FINAL_CONSIDER_A_MATCH) &
        is.na(FINAL_PROBABLE_MATCH) &
        !is.na(upd_FINAL_CIP_CODE_2),
      upd_FINAL_CIP_CODE_2,
      FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      is.na(FINAL_CONSIDER_A_MATCH) &
        is.na(FINAL_PROBABLE_MATCH) &
        !is.na(upd_FINAL_CIP_CODE_2),
      NA_character_,
      FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      is.na(FINAL_CONSIDER_A_MATCH) &
        is.na(FINAL_PROBABLE_MATCH) &
        (!is.na(upd_FINAL_CIP_CODE_4) | !is.na(upd_FINAL_CIP_CODE_2)),
      NA_character_,
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      is.na(FINAL_CONSIDER_A_MATCH) &
        is.na(FINAL_PROBABLE_MATCH) &
        (!is.na(upd_FINAL_CIP_CODE_4) | !is.na(upd_FINAL_CIP_CODE_2)),
      NA_character_,
      FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(-upd_FINAL_CIP_CODE_4, -upd_FINAL_CIP_CODE_2)


## checks
{
  tbl(con, "Credential_Non_Dup_BGS_IDs") %>%
    filter(is.na(FINAL_CIP_CODE_4_NAME)) %>%
    count(FINAL_CONSIDER_A_MATCH, FINAL_PROBABLE_MATCH)
  tbl(con, "Credential_Non_Dup_BGS_IDs") %>%
    filter(is.na(FINAL_CIP_CODE_4_NAME)) %>%
    count(FINAL_CIP_CODE_4)
}

# dbGetQuery(con, qry_fill_final_CIP4_NAME_Credential)
credential_bgs_updated <- credential_bgs_updated %>%
  left_join(
    cip_4_tbl %>%
      select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("FINAL_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4_NAME),
      LCP4_CIP_4DIGITS_NAME,
      FINAL_CIP_CODE_4_NAME
    )
  ) %>%
  select(-LCP4_CIP_4DIGITS_NAME)


# dbGetQuery(con, qry_fill_final_CIP2_NAME_and_CLUSTER_Credential)

credential_bgs_updated <- credential_bgs_updated %>%
  left_join(
    cip_2_tbl %>%
      select(
        LCP2_CD,
        LCP2_DIGITS_NAME,
        LCP2_LCIPPC_CD,
        LCP2_LCIPPC_NAME
      ),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_2_NAME = if_else(
      is.na(FINAL_CIP_CODE_2_NAME),
      LCP2_DIGITS_NAME,
      FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      is.na(FINAL_CIP_CODE_2_NAME),
      LCP2_LCIPPC_CD,
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      is.na(FINAL_CIP_CODE_2_NAME),
      LCP2_LCIPPC_NAME,
      FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(
    -LCP2_DIGITS_NAME,
    -LCP2_LCIPPC_CD,
    -LCP2_LCIPPC_NAME
  )

credential_bgs_updated |> glimpse()
# write it back to SQL

target_name <- "Credential_Non_Dup_BGS_IDs"
temp_name <- "Credential_Non_Dup_BGS_IDs_temp"

if (dbExistsTable(con, Id(schema = my_schema, table = temp_name))) {
  dbRemoveTable(con, Id(schema = my_schema, table = temp_name))
}

credential_bgs_updated <- credential_bgs_updated %>%
  compute(
    name = Id(schema = my_schema, table = temp_name),
    temporary = FALSE
  )

if (dbExistsTable(con, Id(schema = my_schema, table = target_name))) {
  dbRemoveTable(con, Id(schema = my_schema, table = target_name))
}
dbExecute(
  con,
  glue::glue("EXEC sp_rename '{my_schema}.{temp_name}', '{target_name}'")
)

credential_bgs_updated <- tbl(con, in_schema(my_schema, target_name))

credential_bgs_updated |> glimpse()
credential_bgs_updated |> tally()


## check for blanks
{
  tbl(con, "Credential_Non_Dup_BGS_IDs") %>%
    filter(is.na(FINAL_CIP_CODE_4_NAME))
  tbl(con, "Credential_Non_Dup_BGS_IDs") %>%
    filter(is.na(FINAL_CIP_CLUSTER_CODE))
}


## remove local tables
rm(
  Credential_Matched_CIPS_using_BGS,
  Credential_Unmatched_CIPS,
  Credential_Unmatched_CIPS_to_review,
  chk,
  Credential_Unmatched_CIPS_to_update
)

## Part 5: Update T_BGS_DATA_FINAL ----
## Created tables: T_BGS_Data_Final_CIPS_to_update
## Updated tables: T_BGS_Data_Final_CIP_for_OutcomesMatching
# ---- Part 5: Update T_BGS_DATA_FINAL ----
# Created table: T_BGS_Data_Final_for_OutcomesMatching (Updated)
#
# WHAT: Updates the main BGS outcomes table with final matched CIP codes and metadata.
# WHY: The matching workflow determines which CIP codes to use (BGS or STP) for each record.
#      This needs to be reflected in the source outcomes table.
# HOW: 1) Left join matching results to BGS outcomes table
#      2) Use matched CIPs where available, fallback to original BGS CIPs
#      3) Add USE_STP_CIP flag to indicate source of final CIP code
#      4) Enrich with STP CIP codes for comparison
#      5) Fill missing cluster information and finalize
#
# TODO [LOW]: Add validation that final CIP coverage is 100%

### Part 5A: Update with XWALK ----

{
  ## may want to make a backup copy of T_BGS_Data_Final_for_OutcomesMatching
  ## in case you want to make changes to the manual matching
  if (
    dbExistsTable(
      con,
      name = Id(
        schema = my_schema,
        table = "T_BGS_Data_Final_for_OutcomesMatching_bu"
      )
    )
  ) {
    dbRemoveTable(
      con,
      name = Id(
        schema = my_schema,
        table = "T_BGS_Data_Final_for_OutcomesMatching_bu"
      )
    )
  }
  dbExecute(
    con,
    glue::glue(
      "select * into [{my_schema}].T_BGS_Data_Final_for_OutcomesMatching_bu from [{my_schema}].T_BGS_Data_Final_for_OutcomesMatching"
    )
  )
}

## Fill in final CIPS with BGS_Matching_STP_Credential_PEN
dbGetQuery(con, qry_T_BGS_Data_add_columns)


dbGetQuery(con, qry_update_T_BGS_Data_CIP_matches_step1) ## fill in final CIP, etc. from BGS_Matching_STP_Credential_PEN where FINAL_CONSIDER_A_MATCH is not empty


dbGetQuery(con, qry_update_T_BGS_Data_CIP_matches_step2) ## fill in still empty CIP, etc. from BGS_Matching_STP_Credential_PEN where FINAL_PROBABLE_MATCH is not empty


dbGetQuery(con, qry_update_T_BGS_Data_CIP_matches_step3) ## switch USE_BGS_CIP to USE_STP_CIP


dbGetQuery(con, qry_update_T_BGS_Data_CIP_matches_step4) ## drop USE_BGS_CIP

### Part 5B: Update unmatched CIPs ----

## Fill in remaining final CIPS with BGS CIP from T_BGS_Data_Final_for_OutcomesMatching
dbGetQuery(con, qry_update_remaining_BGS_CIPs_in_T_BGS_Data_step1) ## use BGS CIPs as final for remaining


dbGetQuery(con, qry_update_remaining_BGS_CIPs_in_T_BGS_Data_step2) ## fill in CIP_CLUSTER info for remaining

## Create a list of programs that matched to STP data and use STP CIPs instead of BGS
T_BGS_Data_Matched_CIPS_using_STP <- dbGetQuery(
  con,
  qry_List_T_BGS_Data_Using_STP_CIPS
)
## Create a list of programs that did not match to STP data
T_BGS_Data_Unmatched_CIPS <- dbGetQuery(con, qry_List_T_BGS_Data_Umatched)


# new code
t_bgs_updated <- tbl(
  con,
  in_schema(my_schema, "T_BGS_Data_Final_for_OutcomesMatching")
)
t_bgs_updated |> glimpse()


t_bgs_updated <- t_bgs_updated %>%
  left_join(
    bgs_matching_final %>%
      filter(!is.na(FINAL_CONSIDER_A_MATCH) | !is.na(FINAL_PROBABLE_MATCH)) %>%
      select(
        STQU_ID,
        MATCH_FINAL_CIP_4 = FINAL_CIP_CODE_4,
        MATCH_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
        MATCH_FINAL_CIP_2 = FINAL_CIP_CODE_2,
        MATCH_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME,
        MATCH_FINAL_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
        MATCH_FINAL_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
        MATCH_USE_BGS = USE_BGS_CIP,
        MATCH_STP_CIP_4 = STP_FINAL_CIP_CODE_4,
        MATCH_STP_CIP_4_NAME = STP_FINAL_CIP_CODE_4_NAME,
        FINAL_CONSIDER_A_MATCH,
        FINAL_PROBABLE_MATCH
      ),
    by = "STQU_ID"
  )
## qry_update_T_BGS_Data_CIP_matches_step1 ----
## New: mimicking Credential_Non_Dup update code
## step1 fill in FINAL CIP columns from BGS_Matching_STP_Credential_PEN where Final_Consider_A_Match is yes (i.e., where programs were “auto” matched with the flags)
t_bgs_updated <- t_bgs_updated %>%

  ## qry_update_T_BGS_Data_CIP_matches_step2 ----
  ## New: mimicking Credential_Non_Dup update code
  ## step2 fill in FINAL CIP columns from BGS_Matching_STP_Credential_PEN where Final_Probable_Match is yes (i.e., where programs were manually matched). Note these CIPs are updated after the auto matches CIPs so that the auto matches take priority in situations where there are duplicate IDs in BGS_Matching_STP_Credential_PEN.

  ## qry_update_remaining_BGS_CIPs_in_T_BGS_Data_step1 ----
  ## New: mimicking Credential_Non_Dup update code

  mutate(
    FINAL_CIP_CODE_4 = coalesce(MATCH_FINAL_CIP_4, CIP_4DIGIT_NO_PERIOD),
    FINAL_CIP_CODE_4_NAME = coalesce(MATCH_FINAL_CIP_4_NAME, CIP4DIG_NAME),
    FINAL_CIP_CODE_2 = coalesce(MATCH_FINAL_CIP_2, CIP2DIG),
    FINAL_CIP_CODE_2_NAME = coalesce(MATCH_FINAL_CIP_2_NAME, CIP2DIG_NAME),
    FINAL_CIP_CLUSTER_CODE = MATCH_FINAL_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME = MATCH_FINAL_CLUSTER_NAME,
    ## qry_update_T_BGS_Data_CIP_matches_step3 ----
    ## New: update USE_STP_CIP with USE_BGS_CIP
    USE_STP_CIP = if_else(MATCH_USE_BGS == "Yes", "No", "Yes"), # Invert logic
    STP_CIP_CODE_4 = MATCH_STP_CIP_4,
    STP_CIP_CODE_4_NAME = MATCH_STP_CIP_4_NAME
  ) %>%
  mutate(USE_STP_CIP = coalesce(USE_STP_CIP, "No because no match")) %>%
  # Fill missing cluster
  left_join(
    cip_2_tbl %>% select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME) %>%
  # Default remaining to STP
  mutate(
    FINAL_CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, STP_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = coalesce(
      FINAL_CIP_CODE_4_NAME,
      STP_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, STP_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(
      FINAL_CIP_CODE_2_NAME,
      STP_CIP_CODE_2_NAME
    ),
    USE_STP_CIP = coalesce(USE_STP_CIP, "No")
  )

target_name <- "T_BGS_Data_Final_for_OutcomesMatching"
temp_name <- "T_BGS_Data_Final_for_OutcomesMatching_temp"

if (dbExistsTable(con, Id(schema = my_schema, table = temp_name))) {
  dbRemoveTable(con, Id(schema = my_schema, table = temp_name))
}

t_bgs_updated <- t_bgs_updated %>%
  compute(
    name = Id(schema = my_schema, table = temp_name),
    temporary = FALSE
  )

if (dbExistsTable(con, Id(schema = my_schema, table = target_name))) {
  dbRemoveTable(con, Id(schema = my_schema, table = target_name))
}
dbExecute(
  con,
  glue::glue("EXEC sp_rename '{my_schema}.{temp_name}', '{target_name}'")
)

t_bgs_updated <- tbl(con, in_schema(my_schema, target_name))

# ---- Clean up ----
dbExecute(con, "DROP TABLE tmp_manual_updates")
dbDisconnect(con)


## End ----
## remove backup tables
dbRemoveTable(con, "BGS_Matching_STP_Credential_PEN_bu")
dbRemoveTable(con, "Credential_Non_Dup_BGS_IDs_bu")
dbRemoveTable(con, "T_BGS_Data_Final_for_OutcomesMatching_bu")
dbDisconnect(con)
