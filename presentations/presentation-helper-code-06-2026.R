library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(janitor)

# ---- Configure LAN Paths ----
lan <- config::get("lan")
raw_data_file <- glue::glue("{lan}/data/statcan/stat-can-data-export.csv")

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
# examine the differences between the two model versions
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

# plot the r and sql versions against each other, using ggplot2 scatter plot
# add a descriptive title and axis labels, and a red line representing the 1:1 relationship
# ggplot(diff_r_sql, aes(x = VAL.sql, y = VAL.r)) +
#   geom_point(alpha = 0.5) +
#   geom_abline(slope = 1, intercept = 0, color = "red") +
#   labs(
#     title = "Comparison of Model Versions",
#     subtitle = "Each point = one age-group × NOC × region × year",
#     x = "SQL Model Version",
#     y = "R Model Version"
#   ) +
#   theme_minimal()

# Load the required libraries

#p <- ggplot(
#  diff_r_sql,
#  aes(
#    x = VAL.sql,
#    y = VAL.r,
#    text = glue(
#      "NOC: {ENGLISH_NAME}({NOC})
#      Age Grp.: {AGE_GROUP_ROLLUP_LABEL}
#      Region: {CURRENT_REGION_PSSM_NAME_ROLLUP}
#      Proj. Year: {YEAR}
#      Labour Supply (SQL): {VAL.sql}
#      Labour Supply (R): {VAL.r}"
#    )
#  )
#) +
#  geom_point(alpha = 0.5) +
#  geom_abline(slope = 1, intercept = 0, color = "red") +
#  labs(
#    title = "Projected Labour Supply: SQL vs. R Model Implementations",
#    subtitle = "Each point represents a unique age group, NOC, region, and year combination",
#    x = "Projected Supply (SQL Model)",
#    y = "Projected Supply (R Model)"
#  ) +
#  theme_minimal()
#ggplotly(p, tooltip = "text")

# generate a list of NOC aggregations where the difference between the two model
# versions is greater than 1% and the absolute difference is greater than 1
# we only want to look at the NOC level 5 aggregations, which are the most detailed
# diff_r_sql |>
#   filter(p_diff > 1, diff > 0.5) |>
#   filter(NOC_LEVEL == 5, CURRENT_REGION_PSSM_CODE_ROLLUP > 5900) |>
#   group_by(
#     AGE_GROUP_ROLLUP_LABEL,
#     NOC,
#     CURRENT_REGION_PSSM_CODE_ROLLUP,
#     ENGLISH_NAME
#   ) |>
#   summarise(n_years = n(), max_diff = max(diff), .groups = "drop") |>
#   arrange(desc(NOC)) |>
#   distinct(
#     NOC,
#     ENGLISH_NAME,
#     `Difference (N)` = max_diff
#   )

# ---- cbo vs R
# examine the differences between the dbo and R model versions
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

# plot the r and dbo versions against each other, using ggplot2 scatter plot
# add a descriptive title and axis labels, and a red line representing the 1:1 relationship

#p <- ggplot(
#  diff_r_dbo,
#  aes(
#    x = VAL.dbo,
#    y = VAL.r
#  )
#) +
#  geom_point(alpha = 0.5) +
#  geom_abline(slope = 1, intercept = 0, color = "red") +
#  labs(
#    title = "Projected Labour Supply: 2024 SQL vs. R Model Implementations",
#    subtitle = "Each point represents a unique age group, NOC, region, and year combination",
#    x = "Projected Supply (SQL Model, 2024)",
#    y = "Projected Supply (R Model)"
#  ) +
#  theme_minimal()

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

# plot the r and sql versions against each other, using ggplot2 scatter plot
# add a descriptive title and axis labels, and a red line representing the 1:1 relationship
# p <- ggplot(
#   grad_proj_sql_r,
#   aes(
#     x = log(GRADUATES.sql),
#     y = log(GRADUATES.r),
#     text = glue(
#       "Credential: {PSSM_CREDENTIAL}
#       Age Grp.: {AGE_GROUP}
#       Proj. Year: {YEAR}
#       Grads (SQL): {GRADUATES.sql}
#       Grads (R): {GRADUATES.r}"
#     )
#   )
# ) +
#   geom_point(alpha = 0.5) +
#   geom_abline(slope = 1, intercept = 0, color = "red") +
#   labs(
#     title = "Comparison of Graduate Projections",
#     subtitle = "Each point = one credential × year × age group × survey",
#     x = "SQL Graduate Projections",
#     y = "R Graduate Projections"
#   ) +
#   theme_minimal()
#
# ggplotly(p, tooltip = "text")
# generate a list of aggregations where the difference between the two model
# versions is greater than 1% and the absolute difference is greater than 1
# grad_proj_sql_r |>
#   filter(p_diff > 1, diff > 0.5) |>
#   group_by(
#     PSSM_CREDENTIAL,
#     AGE_GROUP
#   ) |>
#   slice_min(n = 1, order_by = YEAR) |>
#   select(
#     PSSM_CREDENTIAL,
#     AGE_GROUP,
#     `Difference (N)` = diff,
#     `Percent Difference (%)` = p_diff
#   )

# ---- dbo vs R
# join the r and dbo versions of the graduate projections, and
# calculate the difference and percent difference
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

# filter the dbo vs R graduate projections to find the differences greater than 1% and absolute difference greater than 1
# grad_proj_dbo_sql |>
#   filter(p_diff > 2 & diff > 1) |>
#   group_by(
#     PSSM_CREDENTIAL,
#     AGE_GROUP
#   ) |>
#   slice_min(n = 1, order_by = YEAR) |>
#   select(-PSI_CREDENTIAL_CATEGORY)
