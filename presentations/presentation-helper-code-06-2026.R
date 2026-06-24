library(tidyverse)
library(odbc)
library(DBI)
library(janitor)

# ----- Connection to decimal ----
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
db_schema <- config::get("dbschema")

con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# get the two model versions from decimal
tmp_tbl_model_sql <- dbReadTable(
  con,
  SQL(glue::glue(
    '"{my_schema}"."tmp_tbl_model"'
  ))
)

tmp_tbl_model_r <- dbReadTable(
  con,
  SQL(glue::glue(
    '"{my_schema}"."tmp_tbl_model_r"'
  ))
)

tmp_tbl_model_dbo <- dbReadTable(
  con,
  SQL(glue::glue(
    '"{db_schema}"."tmp_tbl_model"'
  ))
)

# clean column names
names(tmp_tbl_model_sql) <- toupper(names(tmp_tbl_model_sql))
names(tmp_tbl_model_r) <- toupper(names(tmp_tbl_model_r))
names(tmp_tbl_model_dbo) <- toupper(names(tmp_tbl_model_dbo))

names(tmp_tbl_model_sql) <- gsub("X", "", names(tmp_tbl_model_sql))
names(tmp_tbl_model_sql) <- gsub("\\.", "/", names(tmp_tbl_model_sql))
names(tmp_tbl_model_r) <- gsub("X", "", names(tmp_tbl_model_r))
names(tmp_tbl_model_r) <- gsub("\\.", "/", names(tmp_tbl_model_r))
names(tmp_tbl_model_dbo) <- gsub("X", "", names(tmp_tbl_model_dbo))
names(tmp_tbl_model_dbo) <- gsub("\\.", "/", names(tmp_tbl_model_dbo))

tmp_tbl_model_sql <- tmp_tbl_model_sql |>
  select(any_of(intersect(names(tmp_tbl_model_sql), names(tmp_tbl_model_r))))
tmp_tbl_model_r <- tmp_tbl_model_r |>
  select(any_of(intersect(names(tmp_tbl_model_sql), names(tmp_tbl_model_r))))
tmp_tbl_model_dbo <- tmp_tbl_model_dbo |>
  select(any_of(intersect(names(tmp_tbl_model_dbo), names(tmp_tbl_model_r))))

# transform the data frame to long format, pivoting on the YEAR columns
tmp_tbl_model_sql_long <- tmp_tbl_model_sql |>
  pivot_longer(
    cols = starts_with("20"),
    names_to = "YEAR",
    values_to = "VAL"
  )

tmp_tbl_model_r_long <- tmp_tbl_model_r |>
  pivot_longer(
    cols = starts_with("20"),
    names_to = "YEAR",
    values_to = "VAL"
  )

tmp_tbl_model_dbo_long <- tmp_tbl_model_dbo |>
  pivot_longer(
    cols = starts_with("20"),
    names_to = "YEAR",
    values_to = "VAL"
  )

# ---- myschema vs R
diff_r_sql <- tmp_tbl_model_sql_long |>
  full_join(
    tmp_tbl_model_r_long,
    by = c(
      "AGE_GROUP_ROLLUP_LABEL",
      "NOC_LEVEL",
      "NOC",
      "ENGLISH_NAME",
      "CURRENT_REGION_PSSM_CODE_ROLLUP",
      "CURRENT_REGION_PSSM_NAME_ROLLUP",
      "YEAR"
    ),
    suffix = c(".sql", ".r")
  ) |>
  mutate(
    diff = abs(VAL.sql - VAL.r),
    p_diff = abs(VAL.sql - VAL.r) / ((VAL.sql + VAL.r) / 2) * 100
  )

# ---- dbo vs R
diff_r_dbo <- tmp_tbl_model_dbo_long |>
  full_join(
    tmp_tbl_model_r_long,
    by = c(
      "AGE_GROUP_ROLLUP_LABEL",
      "NOC_LEVEL",
      "NOC",
      "ENGLISH_NAME",
      "CURRENT_REGION_PSSM_CODE_ROLLUP",
      "CURRENT_REGION_PSSM_NAME_ROLLUP",
      "YEAR"
    ),
    suffix = c(".dbo", ".r")
  ) |>
  mutate(
    diff = abs(VAL.dbo - VAL.r),
    p_diff = abs(VAL.dbo - VAL.r) / ((VAL.dbo + VAL.r) / 2) * 100
  )


# ---------------- Graduate Projections ----------------

grad_proj_sql <- dbReadTable(
  con,
  SQL(glue::glue(
    '"{my_schema}"."graduate_projections"'
  ))
)

grad_proj_r <- dbReadTable(
  con,
  SQL(glue::glue(
    '"{my_schema}"."graduate_projections_r"'
  ))
)

grad_proj_dbo <- dbReadTable(
  con,
  SQL(glue::glue(
    '"{db_schema}"."graduate_projections"'
  ))
)

names(grad_proj_sql) <- toupper(names(grad_proj_sql))
names(grad_proj_r) <- toupper(names(grad_proj_r))
names(grad_proj_dbo) <- toupper(names(grad_proj_dbo))

# ---- my schema vs R
# join the sql and R versions of the graduate projections, and
# calculate the difference and percent difference
grad_proj_sql_r <- grad_proj_sql |>
  full_join(
    grad_proj_r,
    by = join_by(
      PSSM_CRED,
      YEAR,
      AGE_GROUP,
      SURVEY,
      PSI_CREDENTIAL_CATEGORY,
      PSSM_CREDENTIAL
    ),
    suffix = c(".sql", ".r")
  ) |>
  mutate(
    diff = abs(GRADUATES.sql - GRADUATES.r),
    p_diff = abs(GRADUATES.sql - GRADUATES.r) /
      ((GRADUATES.sql + GRADUATES.r) / 2) *
      100
  )


grad_proj_dbo_sql <- grad_proj_dbo |>
  full_join(
    grad_proj_sql,
    by = join_by(
      PSSM_CRED,
      YEAR,
      AGE_GROUP,
      SURVEY,
      PSI_CREDENTIAL_CATEGORY,
      PSSM_CREDENTIAL
    ),
    suffix = c(".dbo", ".r")
  ) |>
  mutate(
    GRADUATES.dbo = if_else(is.na(GRADUATES.dbo), 0, round(GRADUATES.dbo, 0)),
    GRADUATES.r = if_else(is.na(GRADUATES.r), 0, round(GRADUATES.r, 0)),
    diff = abs(GRADUATES.dbo - GRADUATES.r),
    p_diff = diff / round(GRADUATES.r, 0) * 100
  )
