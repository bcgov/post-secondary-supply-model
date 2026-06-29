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

# ============================================================================
# WHAT THIS SCRIPT DOES
#   Data-loading partner for 06-program-projections.R. It loads every input that
#   step 06 needs and writes the key ones back to the analyst's IDIR schema as
#   "<name>_r" tables. Inputs come from three places:
#     1. Lookup CSVs on the LAN (development/csv/...).
#     2. Tables already in the analyst's schema (built by earlier steps, e.g.
#        tbl_credential_highest_rank_r, credential_non_dup_r, T_Cohorts_Recoded_r).
#     3. A derived table built here: tbl_program_projection_input (the weighted
#        graduate counts by CIP4 x credential x age x year that drive the program
#        forecast in 06).
#
# RUN BEHAVIOUR
#   The bulk of the loading is wrapped in `if (regular_run | ptib_run)`. The QI
#   run deliberately SKIPS it so the projected/static distributions already in
#   the DB are preserved (QI does not update them).
# ============================================================================

library(tidyverse)
# library(RODBC)   # REMOVED: unused. This script uses DBI/odbc, not RODBC.
library(config)
library(DBI)

# ---- Run flags ----
# Defaults below are for a standalone "regular + PTIB" load (load everything).
# When this script is sourced via the orchestrator (prep-for-*-run.R with
# local = globalenv()), the run flags are inherited from the caller, so these
# assignments are effectively overrides - keep that in mind if results look like
# the wrong run was loaded. (TRUE/FALSE per project convention, not T/F.)
regular_run <- TRUE
ptib_run <- TRUE
qi_run <- FALSE

# ---- Configure LAN and file paths ----
# All environment-specific values come from config.yml (never hardcoded).
# `lan` is the network path to the source CSVs; `my_schema` is this analyst's
# IDIR schema (e.g. "IDIR\NAME") that intermediate tables are written to.
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
# Windows Integrated Authentication (Trusted_Connection = "True") per convention.
db_config <- config::get("decimal")
con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# Load everything for the Regular and PTIB runs; skip for the QI run (which keeps
# the existing projected/static distributions in the DB untouched).
if (regular_run == TRUE | ptib_run == TRUE) {
  # TODO (original author): consider always reloading the lookups and moving this
  # conditional down to where the projected/static distributions are cleared.
  # Trade-off is readability vs. reloading lookups unnecessarily on the QI run.

  # ---- Lookups (rollover horizon + age-group + credential-grouping tables) ----

  # Year-2..Year-12 cohort program distribution rollover factors (used to extend
  # the program mix across the projection horizon in 06).
  t_cohort_program_distributions_y2_to_y12 <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/06/T_Cohort_Program_Distributions_Y2_to_Y12.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps")

  # Apprenticeship Year-2..Year-10 rollover factors (APPSO horizon).
  t_appr_y2_to_y10 <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/06/T_APPR_Y2_to_Y10.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps")

  # Maps near-completer age bands -> graduate-projection age bands.
  tbl_age_groups_near_completers <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/06/tbl_Age_Groups_Near_Completers.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps")

  # Standard age-group code -> label lookup (note: read from the step-07 folder).
  tbl_age_groups <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps")

  # PSSM projection credential grouping (maps raw credential -> projection
  # credential category). Title-case the projection credential so it joins
  # cleanly to PSI_CREDENTIAL_CATEGORY downstream.
  t_pssm_projection_cred_grp <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/06/T_PSSM_Projection_Cred_Grp.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps") |>
    mutate(
      PSSM_PROJECTION_CREDENTIAL = str_to_title(PSSM_PROJECTION_CREDENTIAL)
    )

  # Survey-year weights for the STP-based program distribution (Model selects
  # which weighting vintage to use, e.g. '2023-2024').
  t_weights_stp <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/06/T_Weights_STP.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps")

  # AgeIndex -> AgeGroup lookup. NOT clean_names'd: the original column names
  # (AgeIndex / AgeGroup) are relied on by the join below.
  agegrouplookup <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/06/AgeGroupLookup.csv"
      ),
      col_types = cols(.default = col_guess())
    )

  # ---- Build tbl_Program_Projection_Input ----
  # This is the core derived input for 06: weighted graduate counts by
  # CIP4 x credential x age-group x award-year, scoped to the modelled population.

  # Highest-ranked credential per graduate (built in an earlier step).
  tbl_credential_highest_rank <- dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."tbl_credential_highest_rank_r"'))
  )

  # De-duplicated credential records (carry CIP and source flags per graduate).
  credential_non_dup <- dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."credential_non_dup_r"'))
  )

  # Bring RESEARCH_UNIVERSITY and OUTCOMES_CRED onto the highest-rank table.
  # (Author note: ideally done at the end of 01c-credential-analysis.R; done here
  # because it was missed upstream.)
  tbl_credential_highest_rank <- tbl_credential_highest_rank |>
    left_join(
      credential_non_dup |>
        rename_with(toupper, everything()) |>
        select(ID, RESEARCH_UNIVERSITY, OUTCOMES_CRED),
      by = join_by(ID)
    )

  tbl_program_projection_input <- tbl_credential_highest_rank |>
    select(
      ID,
      AGE_GROUP_AT_GRAD,
      PSI_CREDENTIAL_CATEGORY,
      PSI_AWARD_SCHOOL_YEAR_DELAYED,
      PSI_VISA_STATUS,
      RESEARCH_UNIVERSITY,
      OUTCOMES_CRED
    ) |>
    # Attach the age-group label for each graduate.
    inner_join(
      agegrouplookup |> select(AgeIndex, AgeGroup),
      by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")
    ) |>
    # Attach the 4-digit CIP (and cluster) for each graduate.
    inner_join(
      credential_non_dup |>
        rename_with(toupper, everything()) |>
        select(ID, FINAL_CIP_CODE_4, FINAL_CIP_CLUSTER_CODE),
      by = "ID"
    ) |>
    # Population scope for the program projection:
    #   - drop CIP clusters "09" and "10" (handled outside this stream),
    #   - drop Apprenticeships (modelled separately via the APPSO stream),
    #   - keep only Domestic or unknown-visa graduates, and for research
    #     universities exclude DACSO-sourced outcomes credentials to avoid
    #     double-counting graduates already represented via the DACSO survey.
    filter(
      FINAL_CIP_CLUSTER_CODE != "09",
      FINAL_CIP_CLUSTER_CODE != "10",
      PSI_CREDENTIAL_CATEGORY != 'Apprenticeship',
      (
        # Block 1: Domestic and Research University
        (PSI_VISA_STATUS == "Domestic" &
          RESEARCH_UNIVERSITY == 1 &
          OUTCOMES_CRED != "DACSO") |

          # Block 2: Unknown Status and Research University
          (is.na(PSI_VISA_STATUS) &
            RESEARCH_UNIVERSITY == 1 &
            OUTCOMES_CRED != "DACSO") |

          # Block 3: Domestic and Unknown University
          (PSI_VISA_STATUS == "Domestic" &
            is.na(RESEARCH_UNIVERSITY)) |

          # Block 4: Unknown Status and Unknown University
          (is.na(PSI_VISA_STATUS) &
            is.na(RESEARCH_UNIVERSITY))
      )
    ) |>
    # Collapse to one row per age-group x credential x award-year x CIP4 with a
    # graduate Count. Expr1 is a heritage Access column (credential+agegroup key)
    # carried for parity; Count = n() is weighted downstream in 06.
    summarise(
      Expr1 = first(paste0(PSI_CREDENTIAL_CATEGORY, AgeGroup)),
      Count = n(),
      .by = c(
        "AgeGroup",
        "PSI_CREDENTIAL_CATEGORY",
        "PSI_AWARD_SCHOOL_YEAR_DELAYED",
        "FINAL_CIP_CODE_4"
      )
    )

  # Private-college cohort distribution (from step 05). Column 2 holds the
  # credential; rename it to PSSM_CREDENTIAL so 06 can align it with the other
  # streams.
  qry_private_credentials_06d1_cohort_dist <-
    dbReadTable(
      con,
      SQL(glue::glue(
        '"{my_schema}"."qry_Private_Credentials_06d1_Cohort_Dist_r"'
      ))
    )

  names(qry_private_credentials_06d1_cohort_dist)[2] <- "PSSM_CREDENTIAL"

  # Near-completer ratios by age x CIP4 x trades-training (from step 03).
  dacso_near_completers_ratios_age_at_grad_cip4_ttrain <-
    dbReadTable(
      con,
      SQL(glue::glue(
        '"{my_schema}"."T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_r"'
      ))
    ) |>
    janitor::clean_names(case = "all_caps")

  # Combined recoded student-outcomes cohorts (the program -> CIP -> NOC source
  # table; built in 02b). Used in 06 for the public/grad/apprenticeship streams.
  t_cohorts_recoded <- dbReadTable(
    con,
    SQL(glue::glue(
      '"{my_schema}"."T_Cohorts_Recoded_r"'
    ))
  )

  # ---- Rollover data: define EMPTY projected/static distribution tables ----
  # Hacky pattern (author-flagged): rather than create a schema, read the rollover
  # CSVs purely to inherit their COLUMN STRUCTURE, then empty them with
  # filter(FALSE). 06 fills these incrementally. They are not cleared on the QI
  # run so its existing distributions survive. (A real DB schema would be cleaner.)
  cohort_program_distributions_projected <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/rollover/06/Cohort_Program_Distributions_Projected.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps")

  cohort_program_distributions_static <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/rollover/06/Cohort_Program_Distributions_Static.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps")

  # Empty both tables (keep columns only) and force the three columns that 06
  # writes as character so later bind_rows() don't hit type mismatches.
  cohort_program_distributions_static <- cohort_program_distributions_static |>
    filter(FALSE) |>
    mutate(
      LCIP2_CRED = as.character(LCIP2_CRED),
      TTRAIN = as.character(TTRAIN),
      GRAD_STATUS = as.character(GRAD_STATUS)
    )
  cohort_program_distributions_projected <- cohort_program_distributions_projected |>
    filter(FALSE) |>
    mutate(
      LCIP2_CRED = as.character(LCIP2_CRED),
      TTRAIN = as.character(TTRAIN),
      GRAD_STATUS = as.character(GRAD_STATUS)
    )

  # Guard: T_Cohorts_Recoded must only contain the expected survey years.
  # Fails fast if an unexpected year slipped into the cohort build.
  stopifnot(exprs = {
    t_cohorts_recoded |> distinct(SURVEY_YEAR) |> pull() %in% c(2019:2023)
  })
}


# ---- Student Outcomes Lookups (loaded for ALL runs, incl. QI) ----
# Statistics Canada CIP 2016 code lookups (4- and 6-digit). latin1 encoding is
# required because the source files contain accented characters. These are kept
# in memory for downstream steps (not written to the DB below).
infoware_l_cip_4digits_cip2016 <- readr::read_csv(
  glue::glue(
    "{lan}/development/csv/infoware/INFOWARE_L_CIP_4DIGITS_CIP2016.csv"
  ),
  col_types = cols(
    .default = col_guess()
  ),
  locale = locale(
    encoding = "latin1"
  )
)

infoware_l_cip_6digits_cip2016 <- readr::read_csv(
  glue::glue(
    "{lan}/development/csv/infoware/INFOWARE_L_CIP_6DIGITS_CIP2016.csv"
  ),
  col_types = cols(.default = col_guess()),
  locale = locale(
    encoding = "latin1"
  )
)

## ------------------------------------ Clean Up --------------------------------------------------
# Workflow:
#  - Write the key tables back to SQL Server (those needed by 06 or for later
#    reference). Each is written into the analyst's schema with an "_r" suffix.
#  - (The infoware CIP lookups and intermediate tables like
#    tbl_credential_highest_rank / credential_non_dup are intentionally NOT
#    persisted; they remain in the in-memory environment for the next step.)
#  - Note: the historical comment about removing all objects no longer applies -
#    objects are kept in memory and gc() only releases freed memory.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  # all the lookups read from CSV above, plus the derived input and the two
  # (empty) rollover distribution tables.
  "t_cohort_program_distributions_y2_to_y12",
  "t_appr_y2_to_y10",
  "tbl_age_groups_near_completers",
  "tbl_age_groups",
  "t_pssm_projection_cred_grp",
  "t_weights_stp",
  "agegrouplookup",
  "tbl_program_projection_input",
  "cohort_program_distributions_projected",
  "cohort_program_distributions_static"
)

# Write one named global object to "<schema>"."<table_name>_r", overwriting any
# existing copy. Called over tables_to_keep with walk().
write_table_to_db <- function(table_name, schema, con) {
  db_name <- paste0(table_name, "_r")
  dbWriteTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"')),
    base::get(table_name, envir = .GlobalEnv), # fetch the object by name
    overwrite = TRUE
  )
}

walk(tables_to_keep, write_table_to_db, schema = my_schema, con = con)

# NOTE: this script does not dbDisconnect(con) - the connection is intentionally
# left open for the paired analysis step (06) that runs next in the pipeline.
gc()
