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

# ******************************************************************************
# Load datasets required to run program projections step
# Note: Rollover dataset originally contain these entries in SURVEY:
# Projected
#  - PTIB
#  - Program_Projections_2019-2020_Q014e
#  - Program_Projections_2019-2020_qry10c
#  - Program_Projections_2019-2020_qry12c
#  - Program_Projections_2019-2020_qry_13d
# Static
#  - PTIB
#  - Program_Projections_2019-2020_Q012e
#  - Program_Projections_2019-2020_Q013e
#  - Program_Projections_2019-2020_Q014e
#  - Program_Projections_2019-2020_qry_13d
# ******************************************************************************

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

if (regular_run == T | ptib_run == T) {
  # I think we can probably load all lookups, regardless, and move this conditional to
  # later in the script, to where the projected/static distributions are cleared.

  # ---- Lookups  ----
  t_cohort_program_distributions_y2_to_y12 <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/06/T_Cohort_Program_Distributions_Y2_to_Y12.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps")

  t_appr_y2_to_y10 <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/06/T_APPR_Y2_to_Y10.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps")

  tbl_age_groups_near_completers <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/06/tbl_Age_Groups_Near_Completers.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps")

  tbl_age_groups <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps")

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

  t_weights_stp <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/06/T_Weights_STP.csv"
      ),
      col_types = cols(.default = col_guess())
    ) |>
    janitor::clean_names(case = "all_caps")

  agegrouplookup <-
    readr::read_csv(
      glue::glue(
        "{lan}/development/csv/gh-source/lookups/06/AgeGroupLookup.csv"
      ),
      col_types = cols(.default = col_guess())
    )

  # ---- Build tbl_Program_Projection_Input ----
  tbl_credential_highest_rank <- dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."tblCredential_HighestRank"'))
  )

  credential_non_dup <- dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"'))
  )

  tbl_program_projection_input <- tbl_credential_highest_rank |>
    select(
      id,
      AGE_GROUP_AT_GRAD,
      PSI_CREDENTIAL_CATEGORY,
      PSI_AWARD_SCHOOL_YEAR_DELAYED,
      PSI_VISA_STATUS,
      RESEARCH_UNIVERSITY,
      OUTCOMES_CRED
    ) |>
    inner_join(
      agegrouplookup |> select(AgeIndex, AgeGroup),
      by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")
    ) |>
    inner_join(
      credential_non_dup |>
        select(id, FINAL_CIP_CODE_4, FINAL_CIP_CLUSTER_CODE),
      by = "id"
    ) |>
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

  qry_private_credentials_06d1_cohort_dist <-
    dbReadTable(
      decimal_con,
      SQL(glue::glue(
        '"{my_schema}"."qry_Private_Credentials_06d1_Cohort_Dist"'
      ))
    )

  dacso_near_completers_ratios_age_at_grad_cip4_ttrain <-
    dbReadTable(
      decimal_con,
      SQL(glue::glue(
        '"{my_schema}"."T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN"'
      ))
    ) |>
    janitor::clean_names(case = "all_caps")

  t_cohorts_recoded <- dbReadTable(
    decimal_con,
    SQL(glue::glue(
      '"{my_schema}"."T_Cohorts_Recoded"'
    ))
  )

  # ---- Rollover data ----
  # this whole section is hacky - we are essentially defining a schema in the db.
  # starting with a fresh schema (no rows).  But we don't clear them if we are doing the QI run
  # because we want to keep the projected/static distributions in there, as they are not being updated in the QI run.
  # maybe better to just make a schema instead
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

  # The R version is
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

  # check that only required survey years are in T_Cohorts_Recoded
  stopifnot(exprs = {
    t_cohorts_recoded |> distinct(SURVEY_YEAR) |> pull() == c(2019:2023)
  })
}


# ---- Student Outcomes Lookups ----
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

# ---- Disconnect and Clean Up ----
dbDisconnect(decimal_con)
gc()
