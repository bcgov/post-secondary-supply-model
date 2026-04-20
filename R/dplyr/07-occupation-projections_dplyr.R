# Occupation Projections — dplyr Translation
# Original: R/07-occupation-projections.R (~2949 lines, ~198 SQL ops)
#
# Pipeline context:
#   Computes final occupation projections from graduate projections, program distributions,
#   labour supply distributions, and occupation distributions. Produces pivot tables
#   by NOC level (1D-5D) for reporting.
#
# Major sections:
#   Q_0: Setup (Cohort_Program_Distributions copy, PTIB append/delete, T_LCP2_LCP4)
#   Q_1: Grad projections by age/program (join grads × program dists, exclude flagged programs)
#   Q_2: Labour supply by LCIP4_CRED (cascade: direct → No_TT proxy → Private_Cred proxy → LCP2 proxy)
#   Q_3: Occupations by LCIP4_CRED (cascade: direct → No_TT proxy → Private_Cred proxy → LCP2 proxy)
#   Q_4: NOC pivot tables (1D-5D by PSSM_CRED and by Year)
#   Q_5: BC and Total rollups, UNION with regional data
#   Q_6: Model/QI/PTIB table copies, QI error rate, coverage indicator
#
# Input tables:
#   - Graduate_Projections — projected graduates by PSSM_CRED/Age_Group/Year
#   - Cohort_Program_Distributions (or _Static/_Projected) — program distribution percentages
#   - Labour_Supply_Distribution (4 variants: base, LCP2, No_TT, LCP2_No_TT)
#   - Occupation_Distributions (4 variants: base, LCP2, No_TT, LCP2_No_TT)
#   - T_Exclude_from_Projections_* — exclusion lists
#   - T_NOC_Broad_Categories — NOC hierarchy mapping
#   - T_LCP2_LCP4 — CIP 2-digit to 4-digit mapping
#
# Output tables (written to DB):
#   - Q_1_Grad_Projections_by_Age_by_Program, Q_1c_Grad_Projections_by_Program
#   - tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union (labour supply final)
#   - tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union (occupations final)
#   - Q_4_NOC_*_Totals_by_PSSM_CRED, Q_4_NOC_*_Totals_by_Year (1D-5D pivot tables)
#   - Q_4_NOC_Totals_by_Year, Q_4_NOC_Totals_by_Year_BC, Q_4_NOC_Totals_by_Year_Total
#   - Q_5_NOC_Totals_by_Year_and_BC, Q_5_NOC_Totals_by_Year_and_BC_and_Total
#   - tmp_tbl_Model, tmp_tbl_QI, tmp_tbl_Model_Inc_Private_Inst

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# Helper: write to schema
write_schema_table <- function(name, data) {
  dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."{name}"')), data, overwrite = TRUE)
}

# Helper: compute a private-cred proxy (cross CERT↔DIPL for PTIB P - CERT / P - DIPL)
# WHY: PTIB credentials (P - CERT, P - DIPL) often lack their own distribution data.
# We use the other private credential as a proxy (CERT uses DIPL distributions and vice versa).
private_cred_proxy <- function(unknowns, dist_table, join_vars) {
  unknowns %>%
    inner_join(dist_table, by = join_vars) %>%
    filter(
      (PSSM_CRED == "P - CERT" & str_detect(paste0(PSSM_CRED.y, collapse=""), "P - DIPL")) |
        (PSSM_CRED == "P - DIPL" & str_detect(paste0(PSSM_CRED.y, collapse=""), "P - CERT"))
    )
}


# ******************************************************************************
# Q_0: Setup — Cohort_Program_Distributions, PTIB, T_LCP2_LCP4
# WHY: Copy Static program distributions if the working table doesn't exist.
# Append/delete PTIB data from distribution tables based on run flags.
# ******************************************************************************

# Copy static distributions if needed
# KEPT AS SQL: SELECT INTO across schemas
if (!dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions"')))) {
  dbExecute(decimal_con, "SELECT * INTO Cohort_Program_Distributions FROM Cohort_Program_Distributions_Static;")
}

# T_LCP2_LCP4: CIP 2-digit to 4-digit mapping
if (!dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_LCP2_LCP4"')))) {
  lcp2_lcp4 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
    select(LCIP_LCP2_CD, LCIP_LCP4_CD) %>%
    distinct() %>%
    collect() |> rename_with(toupper)
  write_schema_table("T_LCP2_LCP4", lcp2_lcp4)
} else {
  lcp2_lcp4 <- sch_tbl("T_LCP2_LCP4") %>% collect() |> rename_with(toupper)
}

# PTIB append/delete
# WHY: PTIB (private institution) data needs to be added to or removed from distribution
# tables depending on whether we're running the PTIB model.
# KEPT AS SQL: INSERT INTO and DELETE on existing DB tables (modifying row-level data)
if (ptib_run == TRUE) {
  # Append PTIB rows to 4 distribution tables
  for (tbl_name in c("Labour_Supply_Distribution_No_TT",
                      "Labour_Supply_Distribution_LCP2_No_TT",
                      "Occupation_Distributions_No_TT",
                      "Occupation_Distributions_LCP2_No_TT")) {
    lcp_col <- if (grepl("LCP2", tbl_name)) "LCP2_CD" else "LCP4_CD"
    lcip_col <- if (grepl("LCP2", tbl_name)) "LCP2_CRED" else "LCIP4_CRED"
    existing <- sch_tbl(tbl_name) %>% collect() |> rename_with(toupper)

    ptib_rows <- existing %>%
      filter(PSSM_CREDENTIAL %in% c("CERT", "DIPL", "ADGR or UT", "BACH", "MAST", "DOCT"),
             !str_detect(!!sym(lcip_col), "^3 - ")) %>%
      mutate(
        SURVEY = "PTIB",
        PSSM_CRED = paste0("P - ", PSSM_CREDENTIAL),
        !!lcip_col := paste0("P - ", !!sym(lcp_col), " - ", PSSM_CREDENTIAL)
      )

    dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."{tbl_name}"')),
                 bind_rows(existing, ptib_rows), overwrite = TRUE)
  }
} else {
  # Delete PTIB rows from 4 distribution tables
  for (tbl_name in c("Labour_Supply_Distribution_No_TT",
                      "Labour_Supply_Distribution_LCP2_No_TT",
                      "Occupation_Distributions_No_TT",
                      "Occupation_Distributions_LCP2_No_TT")) {
    dbExecute(decimal_con, glue::glue(
      "DELETE FROM [{my_schema}].[{tbl_name}] WHERE PSSM_CRED LIKE 'P - %';"
    ))
  }
}


# ******************************************************************************
# Q_1: Grad Projections by Age by Program
# WHY: Multiply projected graduates by program distribution percentages to get
# graduates per CIP program. Exclude flagged programs (no outcomes data available).
# ******************************************************************************

grad_projections <- sch_tbl("Graduate_Projections") %>% collect() |> rename_with(toupper)
cohort_prog_dist <- sch_tbl("Cohort_Program_Distributions") %>% collect() |> rename_with(toupper)
exclude_lcp4 <- sch_tbl("T_Exclude_from_Projections_LCP4_CD") %>% collect() |> rename_with(toupper)
exclude_pssm_cred <- sch_tbl("T_Exclude_from_Projections_PSSM_Credential") %>% collect() |> rename_with(toupper)
exclude_lcip4 <- sch_tbl("T_Exclude_from_Projections_LCIP4_CRED") %>% collect() |> rename_with(toupper)

# Exclude lookup sets
exclude_lcp4_set <- exclude_lcp4$LCIP_LCP4_CD
exclude_pssm_set <- exclude_pssm_cred$PSSM_CREDENTIAL
exclude_lcip4_set <- exclude_lcip4$LCIP4_CRED

# Q_1_Grad_Projections_by_Age_by_Program
Q1 <- grad_projections %>%
  inner_join(cohort_prog_dist,
             by = c("PSSM_CRED", "AGE_GROUP" = "AGE_GROUP", "YEAR")) %>%
  mutate(GRADS = GRADUATES * PERCENT) %>%
  filter(
    !LCP4_CD %in% exclude_lcp4_set,
    !PSSM_CREDENTIAL %in% exclude_pssm_set,
    !LCIP4_CRED %in% exclude_lcip4_set
  ) %>%
  select(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP, YEAR, LCP4_CD, GRAD_STATUS,
         TTRAIN, LCIP4_CRED, GRADS)

write_schema_table("Q_1_Grad_Projections_by_Age_by_Program", Q1)

# Also build static version
cohort_prog_static <- sch_tbl("Cohort_Program_Distributions_Static") %>% collect() |> rename_with(toupper)

Q1_static <- grad_projections %>%
  inner_join(cohort_prog_static,
             by = c("PSSM_CRED", "AGE_GROUP" = "AGE_GROUP", "YEAR")) %>%
  mutate(GRADS = GRADUATES * PERCENT) %>%
  filter(
    !LCP4_CD %in% exclude_lcp4_set,
    !PSSM_CREDENTIAL %in% exclude_pssm_set,
    !LCIP4_CRED %in% exclude_lcip4_set
  )

# Q_1c: Roll up to age group rollup level
age_groups <- sch_tbl("tbl_Age_Groups") %>% collect() |> rename_with(toupper)
age_groups_rollup <- sch_tbl("tbl_Age_Groups_Rollup") %>% collect() |> rename_with(toupper)

Q1c <- Q1 %>%
  inner_join(age_groups %>% select(AGE_GROUP_LABEL, AGE_GROUP_ROLLUP),
             by = c("AGE_GROUP" = "AGE_GROUP_LABEL")) %>%
  inner_join(age_groups_rollup %>% select(AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL),
             by = "AGE_GROUP_ROLLUP") %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL,
           YEAR, GRAD_STATUS, TTRAIN, LCP4_CD, LCIP4_CRED) %>%
  summarise(GRADS = sum(GRADS), .groups = "drop")

write_schema_table("Q_1c_Grad_Projections_by_Program", Q1c)


# ******************************************************************************
# Q_2: Labour Supply by LCIP4_CRED
# WHY: Multiply grads by labour supply distribution (NLS) percentages.
# Uses cascading fallback: direct match → No_TT proxy → Private_Cred proxy → LCP2 proxy.
# Each step resolves "unknowns" (programs without a direct match) using progressively
# broader approximations.
# ******************************************************************************

ls_dist <- sch_tbl("Labour_Supply_Distribution") %>% collect() |> rename_with(toupper)
ls_dist_no_tt <- sch_tbl("Labour_Supply_Distribution_No_TT") %>% collect() |> rename_with(toupper)
ls_dist_lcp2 <- sch_tbl("Labour_Supply_Distribution_LCP2") %>% collect() |> rename_with(toupper)
ls_dist_lcp2_no_tt <- sch_tbl("Labour_Supply_Distribution_LCP2_No_TT") %>% collect() |> rename_with(toupper)
exclude_lcp2_proxy <- sch_tbl("T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy") %>%
  collect() |> rename_with(toupper)

# ---- Step 1: Direct match with Labour_Supply_Distribution ----
Q2_direct <- Q1c %>%
  inner_join(
    ls_dist %>% select(LCIP4_CRED, AGE_GROUP_ROLLUP, NEW_LABOUR_SUPPLY, CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP")
  ) %>%
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY)

# Unknowns after direct match
Q2_unknowns <- Q1c %>%
  anti_join(Q2_direct, by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP")) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL,
           TTRAIN, LCP4_CD, LCIP4_CRED, YEAR) %>%
  summarise(GRADS = sum(GRADS), .groups = "drop")

# ---- Step 2: No_TT proxy (match on LCP4_CD, AGE_GROUP_ROLLUP, PSSM_CRED without TTRAIN) ----
Q2_no_tt_proxy <- Q2_unknowns %>%
  inner_join(
    Q1c %>% select(PSSM_CRED, AGE_GROUP_ROLLUP, LCIP4_CRED, YEAR),
    by = c("PSSM_CRED", "AGE_GROUP_ROLLUP", "LCIP4_CRED", "YEAR")
  ) %>%
  inner_join(
    ls_dist_no_tt %>% select(LCIP4_CRED, AGE_GROUP_ROLLUP, NEW_LABOUR_SUPPLY,
                              CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP")
  ) %>%
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY)

Q2_after_no_tt <- bind_rows(Q2_direct, Q2_no_tt_proxy)

# Unknowns after No_TT
Q2_unknowns_2 <- Q1c %>%
  anti_join(Q2_after_no_tt, by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP"))

# ---- Step 3: Private_Cred proxy (P-CERT uses P-DIPL distributions, and vice versa) ----
Q2_private_proxy <- Q2_unknowns_2 %>%
  inner_join(
    Q1c %>% select(PSSM_CRED, AGE_GROUP_ROLLUP, LCIP4_CRED, YEAR),
    by = c("PSSM_CRED", "AGE_GROUP_ROLLUP", "LCIP4_CRED", "YEAR")
  ) %>%
  inner_join(
    ls_dist_no_tt %>% select(PSSM_CRED, LCP4_CD, AGE_GROUP_ROLLUP,
                              NEW_LABOUR_SUPPLY, CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = c("AGE_GROUP_ROLLUP", "LCP4_CD")
  ) %>%
  filter(
    (PSSM_CRED.x == "P - CERT" & PSSM_CRED.y == "P - DIPL") |
      (PSSM_CRED.x == "P - DIPL" & PSSM_CRED.y == "P - CERT")
  ) %>%
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY)

Q2_after_private <- bind_rows(Q2_after_no_tt, Q2_private_proxy)

# Unknowns after Private_Cred
Q2_unknowns_3 <- Q1c %>%
  anti_join(Q2_after_private, by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP"))

# ---- Step 4: LCP2 proxy (match on 2-digit CIP code) ----
Q2_lcp2_proxy <- Q2_unknowns_3 %>%
  inner_join(lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  anti_join(exclude_lcp2_proxy, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  inner_join(
    ls_dist_lcp2 %>% select(PSSM_CRED, LCP2_CD, AGE_GROUP_ROLLUP,
                              NEW_LABOUR_SUPPLY, CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = c("PSSM_CRED", "LCIP_LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP")
  ) %>%
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY)

Q2_after_lcp2 <- bind_rows(Q2_after_private, Q2_lcp2_proxy)

# Unknowns after LCP2
Q2_unknowns_4 <- Q1c %>%
  anti_join(Q2_after_lcp2, by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP"))

# ---- Step 5: LCP2 No_TT proxy ----
Q2_lcp2_no_tt_proxy <- Q2_unknowns_4 %>%
  inner_join(lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  anti_join(exclude_lcp2_proxy, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  inner_join(
    ls_dist_lcp2_no_tt %>% select(PSSM_CRED, LCP2_CD, AGE_GROUP_ROLLUP,
                                    NEW_LABOUR_SUPPLY, CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = c("PSSM_CRED", "LCIP_LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP")
  ) %>%
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY)

Q2_after_lcp2_no_tt <- bind_rows(Q2_after_lcp2, Q2_lcp2_no_tt_proxy)

# Unknowns after LCP2 No_TT
Q2_unknowns_5 <- Q1c %>%
  anti_join(Q2_after_lcp2_no_tt, by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP"))

# ---- Step 6: LCP2 Private_Cred proxy ----
Q2_lcp2_private_proxy <- Q2_unknowns_5 %>%
  inner_join(lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  inner_join(
    ls_dist_lcp2_no_tt %>% select(PSSM_CRED, LCP2_CD, AGE_GROUP_ROLLUP,
                                    NEW_LABOUR_SUPPLY, CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = c("PSSM_CRED", "LCIP_LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP")
  ) %>%
  filter(
    (PSSM_CRED.x == "P - CERT" & PSSM_CRED.y == "P - DIPL") |
      (PSSM_CRED.x == "P - DIPL" & PSSM_CRED.y == "P - CERT")
  ) %>%
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY)

# Final labour supply union
labour_supply_final <- bind_rows(Q2_after_lcp2_no_tt, Q2_lcp2_private_proxy) %>%
    rename_with(toupper)

# Remaining unknowns (tracked but not resolved)
Q2_final_unknowns <- Q1c %>%
  anti_join(labour_supply_final, by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP"))

write_schema_table("tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union", labour_supply_final)
write_schema_table("Q_2f_Labour_Supply", Q2_final_unknowns)


# ******************************************************************************
# Q_3: Occupations by LCIP4_CRED
# WHY: Multiply labour supply (NLS) by occupation distribution percentages to get
# projected new labour supply per occupation (NOC). Same cascading fallback pattern
# as Q_2 for resolving unknowns.
# ******************************************************************************

occ_dist <- sch_tbl("Occupation_Distributions") %>% collect() |> rename_with(toupper)
occ_dist_no_tt <- sch_tbl("Occupation_Distributions_No_TT") %>% collect() |> rename_with(toupper)
occ_dist_lcp2 <- sch_tbl("Occupation_Distributions_LCP2") %>% collect() |> rename_with(toupper)
occ_dist_lcp2_no_tt <- sch_tbl("Occupation_Distributions_LCP2_No_TT") %>% collect() |> rename_with(toupper)

# ---- Step 1: Direct match with Occupation_Distributions ----
Q3_direct <- labour_supply_final %>%
  inner_join(
    occ_dist %>% select(LCIP4_CRED, AGE_GROUP_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP, NOC, PERCENT),
    by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP")
  ) %>%
  mutate(OCCSN = NLS * PERCENT)

# Unknowns
Q3_unknowns <- labour_supply_final %>%
  anti_join(
    Q3_direct,
    by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP")
  )

# ---- Step 2: No_TT proxy ----
Q3_no_tt_proxy <- Q3_unknowns %>%
  inner_join(
    occ_dist_no_tt %>% select(PSSM_CRED, LCP4_CD, AGE_GROUP_ROLLUP,
                               CURRENT_REGION_PSSM_CODE_ROLLUP, NOC, PERCENT),
    by = c("PSSM_CRED", "LCP4_CD", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP")
  ) %>%
  mutate(OCCSN = NLS * PERCENT)

Q3_after_no_tt <- bind_rows(Q3_direct, Q3_no_tt_proxy)

Q3_unknowns_2 <- labour_supply_final %>%
  anti_join(Q3_after_no_tt,
            by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP"))

# ---- Step 3: Private_Cred proxy ----
Q3_private_proxy <- Q3_unknowns_2 %>%
  inner_join(
    labour_supply_final %>% select(PSSM_CRED, LCP4_CD, AGE_GROUP_ROLLUP,
                                    CURRENT_REGION_PSSM_CODE_ROLLUP, YEAR),
    by = c("PSSM_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP", "YEAR")
  ) %>%
  inner_join(
    occ_dist_no_tt %>% select(LCP4_CD, AGE_GROUP_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP, NOC, PERCENT),
    by = c("LCP4_CD", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP")
  ) %>%
  filter(
    (PSSM_CRED.x == "P - CERT" & PSSM_CRED.y == "P - DIPL") |
      (PSSM_CRED.x == "P - DIPL" & PSSM_CRED.y == "P - CERT")
  ) %>%
  mutate(OCCSN = NLS * PERCENT)

Q3_after_private <- bind_rows(Q3_after_no_tt, Q3_private_proxy)

Q3_unknowns_3 <- labour_supply_final %>%
  anti_join(Q3_after_private,
            by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP", "YEAR"))

# ---- Step 4: LCP2 proxy ----
Q3_lcp2_proxy <- Q3_unknowns_3 %>%
  inner_join(lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  anti_join(exclude_lcp2_proxy, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  inner_join(
    occ_dist_lcp2 %>% select(PSSM_CRED, LCP2_CD, AGE_GROUP_ROLLUP,
                              CURRENT_REGION_PSSM_CODE_ROLLUP, NOC, PERCENT),
    by = c("PSSM_CRED", "LCIP_LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP",
           "CURRENT_REGION_PSSM_CODE_ROLLUP")
  ) %>%
  mutate(OCCSN = NLS * PERCENT)

Q3_after_lcp2 <- bind_rows(Q3_after_private, Q3_lcp2_proxy)

Q3_unknowns_4 <- labour_supply_final %>%
  anti_join(Q3_after_lcp2,
            by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP", "YEAR"))

# ---- Step 5: LCP2 No_TT proxy ----
Q3_lcp2_no_tt_proxy <- Q3_unknowns_4 %>%
  inner_join(lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  anti_join(exclude_lcp2_proxy, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  inner_join(
    occ_dist_lcp2_no_tt %>% select(PSSM_CRED, LCP2_CD, AGE_GROUP_ROLLUP,
                                     CURRENT_REGION_PSSM_CODE_ROLLUP, NOC, PERCENT),
    by = c("PSSM_CRED", "LCIP_LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP",
           "CURRENT_REGION_PSSM_CODE_ROLLUP")
  ) %>%
  mutate(OCCSN = NLS * PERCENT)

Q3_after_lcp2_no_tt <- bind_rows(Q3_after_lcp2, Q3_lcp2_no_tt_proxy)

Q3_unknowns_5 <- labour_supply_final %>%
  anti_join(Q3_after_lcp2_no_tt,
            by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP", "YEAR"))

# ---- Step 6: LCP2 Private_Cred proxy ----
Q3_lcp2_private_proxy <- Q3_unknowns_5 %>%
  inner_join(lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  inner_join(
    occ_dist_lcp2_no_tt %>% select(PSSM_CRED, LCP2_CD, AGE_GROUP_ROLLUP,
                                     CURRENT_REGION_PSSM_CODE_ROLLUP, NOC, PERCENT),
    by = c("PSSM_CRED", "LCIP_LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP",
           "CURRENT_REGION_PSSM_CODE_ROLLUP")
  ) %>%
  filter(
    (PSSM_CRED.x == "P - CERT" & PSSM_CRED.y == "P - DIPL") |
      (PSSM_CRED.x == "P - DIPL" & PSSM_CRED.y == "P - CERT")
  ) %>%
  mutate(OCCSN = NLS * PERCENT)

Q3_after_lcp2_private <- bind_rows(Q3_after_lcp2_no_tt, Q3_lcp2_private_proxy)

# ---- Step 7: Remaining unknowns get NOC=99999, Percent=1 ----
Q3_unknowns_final <- labour_supply_final %>%
  anti_join(Q3_after_lcp2_private,
            by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP", "YEAR")) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL,
            YEAR, TTRAIN, LCP4_CD, LCIP4_CRED, CURRENT_REGION_PSSM_CODE_ROLLUP) %>%
  summarise(NLS = sum(NLS), .groups = "drop") %>%
  filter(NLS > 0) %>%
  mutate(NOC = 99999, PERCENT = 1, OCCSN = NLS)

# Final occupation union (filter positive OccsN)
occupations_final <- bind_rows(Q3_after_lcp2_private, Q3_unknowns_final) %>%
  filter(OCCSN > 0) %>%
  rename_with(toupper)

write_schema_table("tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union", occupations_final)


# ******************************************************************************
# Q_4: NOC Pivot Tables (1D-5D)
# WHY: Create pivot tables aggregating occupation projections by NOC hierarchy level
# (1-digit broad category through 5-digit unit group) and by PSSM_CRED or Year.
# These are the final output tables for reporting.
# NOTE: The original uses SQL PIVOT which we translate to tidyr::pivot_wider.
# ******************************************************************************

noc_broad <- sch_tbl("T_NOC_Broad_Categories") %>% collect() |> rename_with(toupper)

# Join NOC categories to get hierarchy codes
occ_with_noc <- occupations_final %>%
  inner_join(noc_broad, by = c("NOC" = "UNIT_GROUP_CODE"))

# Helper: build NOC pivot by PSSM_CRED
# WHY: Each NOC level (1D=1-digit, 2D=2-digit, etc.) produces a separate pivot table.
build_noc_pivot_by_cred <- function(data, noc_col, eng_col) {
  pssm_creds <- unique(data$PSSM_CRED)
  data %>%
    mutate(
      NOC_LEVEL = nchar(!!sym(noc_col)),
      NOC = !!sym(noc_col),
      ENGLISH_NAME = !!sym(eng_col)
    ) %>%
    select(PSSM_CRED, OCCSN, NOC_LEVEL, NOC, ENGLISH_NAME) %>%
    pivot_wider(names_from = PSSM_CRED, values_from = OCCSN,
                values_fn = sum, values_fill = 0) %>%
    arrange(NOC_LEVEL, NOC)
}

# Helper: build NOC pivot by Year
# WHY: Separate pivot tables show projections by year for each NOC level.
build_noc_pivot_by_year <- function(data, noc_col, eng_col) {
  data %>%
    mutate(
      NOC_LEVEL = nchar(!!sym(noc_col)),
      NOC = !!sym(noc_col),
      ENGLISH_NAME = !!sym(eng_col)
    ) %>%
    select(YEAR, OCCSN, AGE_GROUP_ROLLUP_LABEL, NOC_LEVEL, NOC, ENGLISH_NAME,
           CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_NAME_ROLLUP) %>%
    pivot_wider(names_from = YEAR, values_from = OCCSN,
                values_fn = sum, values_fill = NA) %>%
    arrange(NOC_LEVEL, NOC)
}

# 1D - Broad Category (1-digit NOC)
Q4_1D_by_cred <- build_noc_pivot_by_cred(occ_with_noc, "BROAD_CATEGORY_CODE", "BROAD_CATEGORY_ENGLISH_NAME")
Q4_1D_by_year <- build_noc_pivot_by_year(occ_with_noc, "BROAD_CATEGORY_CODE", "BROAD_CATEGORY_ENGLISH_NAME")

# 2D - Major Group (2-digit NOC)
Q4_2D_by_cred <- build_noc_pivot_by_cred(occ_with_noc, "MAJOR_GROUP_CODE", "MAJOR_GROUP_ENGLISH_NAME")
Q4_2D_by_year <- build_noc_pivot_by_year(occ_with_noc, "MAJOR_GROUP_CODE", "MAJOR_GROUP_ENGLISH_NAME")

# 3D - Sub-Major Group (3-digit NOC)
Q4_3D_by_cred <- build_noc_pivot_by_cred(occ_with_noc, "SUB_MAJOR_GROUP_CODE", "SUB_MAJOR_ENGLISH_NAME")
Q4_3D_by_year <- build_noc_pivot_by_year(occ_with_noc, "SUB_MAJOR_GROUP_CODE", "SUB_MAJOR_ENGLISH_NAME")

# 4D - Minor Group (4-digit NOC)
Q4_4D_by_cred <- build_noc_pivot_by_cred(occ_with_noc, "MINOR_GROUP_CODE", "MINOR_GROUP_ENGLISH_NAME")
Q4_4D_by_year <- build_noc_pivot_by_year(occ_with_noc, "MINOR_GROUP_CODE", "MINOR_GROUP_ENGLISH_NAME")

# 5D - Unit Group (5-digit NOC)
Q4_5D_by_cred <- build_noc_pivot_by_cred(occ_with_noc, "UNIT_GROUP_CODE", "ENGLISH_NAME")
Q4_5D_by_year <- build_noc_pivot_by_year(occ_with_noc, "UNIT_GROUP_CODE", "ENGLISH_NAME")

# Write all pivot tables
write_schema_table("Q_4_NOC_1D_Totals_by_PSSM_CRED", Q4_1D_by_cred)
write_schema_table("Q_4_NOC_1D_Totals_by_Year", Q4_1D_by_year)
write_schema_table("Q_4_NOC_2D_Totals_by_PSSM_CRED", Q4_2D_by_cred)
write_schema_table("Q_4_NOC_2D_Totals_by_Year", Q4_2D_by_year)
write_schema_table("Q_4_NOC_3D_Totals_by_PSSM_CRED", Q4_3D_by_cred)
write_schema_table("Q_4_NOC_3D_Totals_by_Year", Q4_3D_by_year)
write_schema_table("Q_4_NOC_4D_Totals_by_PSSM_CRED", Q4_4D_by_cred)
write_schema_table("Q_4_NOC_4D_Totals_by_Year", Q4_4D_by_year)
write_schema_table("Q_4_NOC_5D_Totals_by_PSSM_CRED", Q4_5D_by_cred)
write_schema_table("Q_4_NOC_5D_Totals_by_Year", Q4_5D_by_year)

# Union all NOC levels by Year
Q4_totals_by_year <- bind_rows(
  Q4_1D_by_year, Q4_2D_by_year, Q4_3D_by_year, Q4_4D_by_year, Q4_5D_by_year
)
write_schema_table("Q_4_NOC_Totals_by_Year", Q4_totals_by_year)

# ---- BC Rollup ----
# WHY: Aggregate regional NOC projections up to the BC level. The rollup_codes_bc
# table maps individual region codes to their BC-level equivalents, and rollup_codes
# provides the BC-level code and name. Year columns are summed across regions.
rollup_codes_bc <- sch_tbl("T_Current_Region_PSSM_Rollup_Codes_BC") %>%
  collect() |> rename_with(toupper)
rollup_codes <- sch_tbl("T_Current_Region_PSSM_Rollup_Codes") %>%
  collect() |> rename_with(toupper)

# Identify year columns (used for summarise across)
year_cols <- names(Q4_totals_by_year)[grepl("^\\d{4}/\\d{4}$", names(Q4_totals_by_year))]

Q4_totals_by_year_bc <- Q4_totals_by_year %>%
  inner_join(rollup_codes_bc %>% select(CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP_BC),
             by = "CURRENT_REGION_PSSM_CODE_ROLLUP") %>%
  inner_join(rollup_codes %>% select(CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_NAME_ROLLUP),
             by = c("CURRENT_REGION_PSSM_CODE_ROLLUP_BC" = "CURRENT_REGION_PSSM_CODE_ROLLUP")) %>%
  mutate(Expr1000 = paste0(AGE_GROUP_ROLLUP_LABEL, "-", NOC, "-", CURRENT_REGION_PSSM_CODE_ROLLUP_BC)) %>%
  rename(CURRENT_REGION_PSSM_CODE_ROLLUP_ORIG = CURRENT_REGION_PSSM_CODE_ROLLUP,
         CURRENT_REGION_PSSM_NAME_ROLLUP_ORIG = CURRENT_REGION_PSSM_NAME_ROLLUP,
         CURRENT_REGION_PSSM_CODE_ROLLUP = CURRENT_REGION_PSSM_CODE_ROLLUP_BC) %>%
  select(-CURRENT_REGION_PSSM_CODE_ROLLUP_ORIG, -CURRENT_REGION_PSSM_NAME_ROLLUP_ORIG) %>%
  group_by(Expr1000, AGE_GROUP_ROLLUP_LABEL, NOC_LEVEL, NOC, ENGLISH_NAME,
           CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_NAME_ROLLUP) %>%
  summarise(across(all_of(year_cols), ~sum(., na.rm = TRUE)), .groups = "drop")

write_schema_table("Q_4_NOC_Totals_by_Year_BC", Q4_totals_by_year_bc)

# ---- Total Rollup ----
# WHY: Same as BC rollup but uses current_region_pssm_code_rollup_total to aggregate
# all regions into a single provincial total.
Q4_totals_by_year_total <- Q4_totals_by_year %>%
  inner_join(rollup_codes_bc %>% select(CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP_TOTAL),
             by = "CURRENT_REGION_PSSM_CODE_ROLLUP") %>%
  inner_join(rollup_codes %>% select(CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_NAME_ROLLUP),
             by = c("CURRENT_REGION_PSSM_CODE_ROLLUP_TOTAL" = "CURRENT_REGION_PSSM_CODE_ROLLUP")) %>%
  mutate(Expr1000 = paste0(AGE_GROUP_ROLLUP_LABEL, "-", NOC, "-", CURRENT_REGION_PSSM_CODE_ROLLUP_TOTAL)) %>%
  rename(CURRENT_REGION_PSSM_CODE_ROLLUP_ORIG = CURRENT_REGION_PSSM_CODE_ROLLUP,
         CURRENT_REGION_PSSM_NAME_ROLLUP_ORIG = CURRENT_REGION_PSSM_NAME_ROLLUP,
         CURRENT_REGION_PSSM_CODE_ROLLUP = CURRENT_REGION_PSSM_CODE_ROLLUP_TOTAL) %>%
  select(-CURRENT_REGION_PSSM_CODE_ROLLUP_ORIG, -CURRENT_REGION_PSSM_NAME_ROLLUP_ORIG) %>%
  group_by(Expr1000, AGE_GROUP_ROLLUP_LABEL, NOC_LEVEL, NOC, ENGLISH_NAME,
           CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_NAME_ROLLUP) %>%
  summarise(across(all_of(year_cols), ~sum(., na.rm = TRUE)), .groups = "drop")

write_schema_table("Q_4_NOC_Totals_by_Year_Total", Q4_totals_by_year_total)


# ******************************************************************************
# Q_5: BC and Total rollups + UNION
# WHY: Combine regional data with BC-level and Total-level rollups. The final output
# table contains all three aggregation levels: individual regions, BC subtotal,
# and provincial total.
# ******************************************************************************

# Q_5_NOC_Totals_by_Year_and_BC = regional + BC rollup
Q5_bc <- bind_rows(Q4_totals_by_year, Q4_totals_by_year_bc)
write_schema_table("Q_5_NOC_Totals_by_Year_and_BC", Q5_bc)

# Q_5_NOC_Totals_by_Year_and_BC_and_Total = regional + BC + Total rollup
Q5_bc_total <- bind_rows(Q4_totals_by_year, Q4_totals_by_year_bc, Q4_totals_by_year_total)
write_schema_table("Q_5_NOC_Totals_by_Year_and_BC_and_Total", Q5_bc_total)


# ******************************************************************************
# Q_6: Model / QI / PTIB table copies
# WHY: Store final model output under different names for each model run type.
# The regular model, QI model, and PTIB model each get their own copy.
# ******************************************************************************

if (regular_run == TRUE) {
  write_schema_table("tmp_tbl_Model", Q5_bc_total)
}

if (qi_run == TRUE) {
  write_schema_table("tmp_tbl_QI", Q5_bc_total)
}

if (ptib_run == TRUE) {
  write_schema_table("tmp_tbl_Model_Inc_Private_Inst", Q5_bc_total)
}


# ---- Clean up ----
# Drop intermediate Q_2 temp tables (they're now R variables)
# Keep final output tables: Q_1, Q_1c, tmp_tbl_Q_2d, tmp_tbl_Q_3d, Q_4_*, Q_5_*

dbDisconnect(decimal_con)
