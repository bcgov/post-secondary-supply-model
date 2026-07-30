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

pacman::p_load(
  tidyverse,
  RODBC,
  odbc,
  DBI,
  glue,
  RJDBC,
  dbplyr
)


# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
share_schema <- 
# Connect to Decimal
con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "TRUE"
)

# only if those table are not in the analyst's schema, we load those tables

# ---- Read in INFOWARE tables ----
# only run once to get tables ready
iw_config <- config::get("infoware")
odbcListDrivers()

iw_con <- dbConnect(
  odbc::odbc(),
  Driver = "Oracle in instantclient_19c",
  DBQ = iw_config$dbq,
  UID = iw_config$uid,
  PWD = iw_config$pwd
)

# ## ** NOTE **
# ## Ideally match on all data but prioritizing the most recent 6 years - see documentation
# ## Update which BGS_DIST tables to include.

# ## Run the following to get a list of all tables available
# alltables_Infoware <- dbReadTable(iw_con,"ALL_TABLES")
cip_table_names <- alltables_Infoware |> 
  filter(str_detect(TABLE_NAME, "^L_CIP_")) |> 
  pull(TABLE_NAME)



# ---- Tables to copy from INFOWARE to Decimal ----
table_names <- c(
  "BGS_DIST_19_23",
  "BGS_DIST_18_22",
  "BGS_COHORT_INFO",
  "L_CIP_6DIGITS_CIP2016",
  "L_CIP_4DIGITS_CIP2016",
  "L_CIP_2DIGITS_CIP2016",
  "PROGRAMS",
  "PROGRAMS_BKUP_NOV_2024_CIP2016_CIP2021",
  "PROGRAMS_HIST_PRGMID_XREF"
)

# ---- Function to copy INFOWARE tables to Decimal ----
copy_infoware_tables <- function(tbl_name, source_con, dest_con, source_schema = "INFOWARE", dest_schema = my_schema) {

    dest_table <- paste0("INFOWARE_", tbl_name)

    if (!dbExistsTable(dest_con, DBI::Id(schema = dest_schema, table = dest_table))) {
      cat("Copying", tbl_name, "...\n")
      data <- dbReadTable(source_con, DBI::Id(schema = source_schema, table = tbl_name))
      dbWriteTable(dest_con, DBI::Id(schema = dest_schema, table = dest_table), data)
      cat("  Copied", nrow(data), "rows\n")
      rm(data)
    } else {
      cat("Skipping", dest_table, "- already exists\n")
    }
  }

for (tbl_name in table_names) {
  copy_infoware_tables(tbl_name, iw_con, con)
}



dbDisconnect(iw_con)

dbDisconnect(con)

# ## check tables loaded correctly
# {
#   nrow <- tbl(con, "INFOWARE_BGS_DIST_19_23") %>% tally()
#   nrow ## how many rows?
#   tbl(con, "INFOWARE_BGS_DIST_19_23") %>% distinct(STQU_ID) %>% tally() ## are all IDs unique?

#   nrow <- tbl(con, "INFOWARE_BGS_DIST_18_22") %>% tally()
#   nrow ## how many rows?
#   tbl(con, "INFOWARE_BGS_DIST_18_22") %>% distinct(STQU_ID) %>% tally() ## are all IDs unique?

#   nrow <- tbl(con, "INFOWARE_BGS_COHORT_INFO") %>% tally()
#   nrow ## how many rows?
#   tbl(con, "INFOWARE_BGS_COHORT_INFO") %>% distinct(STQU_ID) %>% tally() ## are all IDs unique?

#   rm(nrow)
# }

# ## remove tables and use decimal versions for remainder of code
# rm(
#   INFOWARE_BGS_DIST_19_23,
#   INFOWARE_BGS_DIST_18_22,
#   INFOWARE_BGS_COHORT_INFO,
#   INFOWARE_L_CIP_6DIGITS_CIP2016,
#   INFOWARE_L_CIP_4DIGITS_CIP2016,
#   INFOWARE_L_CIP_2DIGITS_CIP2016
# )


# ---- Write initial tables to Decimal ----
## Save static versions of the INFOWARE tables and last cycle XWALK to Decimal
# !! UPDATE THE TABLES AND ROW NUMBERS !! - connection won't write the full datasets to decimal due to size

if (
  !dbExistsTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_BGS_DIST_19_23")
  )
) {
  INFOWARE_BGS_DIST_19_23 <- dbReadTable(
    iw_con,
    DBI::Id(schema = "INFOWARE", table = "BGS_DIST_19_23")
  )

  dbWriteTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_BGS_DIST_19_23"),
    INFOWARE_BGS_DIST_19_23[1:80000, ]
  )

  dbWriteTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_BGS_DIST_19_23"),
    INFOWARE_BGS_DIST_19_23[80001:121074, ],
    append = TRUE
  )
}

if (
  !dbExistsTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_BGS_DIST_18_22")
  )
) {
  INFOWARE_BGS_DIST_18_22 <- dbReadTable(
    iw_con,
    DBI::SQL("INFOWARE.BGS_DIST_18_22")
  )

  dbWriteTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_BGS_DIST_18_22"),
    INFOWARE_BGS_DIST_18_22[1:80000, ]
  )
  dbWriteTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_BGS_DIST_18_22"),
    INFOWARE_BGS_DIST_18_22[80001:118632, ],
    append = TRUE
  )
}


if (
  !dbExistsTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_BGS_COHORT_INFO")
  )
) {
  INFOWARE_BGS_COHORT_INFO <- dbReadTable(
    iw_con,
    DBI::SQL("INFOWARE.BGS_COHORT_INFO")
  )
  dbWriteTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_BGS_COHORT_INFO"),
    INFOWARE_BGS_COHORT_INFO[1:80000, ]
  )
  dbWriteTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_BGS_COHORT_INFO"),
    INFOWARE_BGS_COHORT_INFO[80001:160000, ],
    append = TRUE
  )
  dbWriteTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_BGS_COHORT_INFO"),
    INFOWARE_BGS_COHORT_INFO[160001:240000, ],
    append = TRUE
  )
  dbWriteTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_BGS_COHORT_INFO"),
    INFOWARE_BGS_COHORT_INFO[240001:290758, ],
    append = TRUE
  )
}

if (
  !dbExistsTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_L_CIP_6DIGITS_CIP2016")
  )
) {
  INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbReadTable(
    iw_con,
    DBI::Id(schema = "INFOWARE", table = "L_CIP_6DIGITS_CIP2016")
  )
  dbWriteTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_L_CIP_6DIGITS_CIP2016"),
    INFOWARE_L_CIP_6DIGITS_CIP2016
  )
}

if (
  !dbExistsTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_L_CIP_4DIGITS_CIP2016")
  )
) {
  INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbReadTable(
    iw_con,
    DBI::Id(schema = "INFOWARE", table = "L_CIP_4DIGITS_CIP2016")
  )
  dbWriteTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_L_CIP_4DIGITS_CIP2016"),
    INFOWARE_L_CIP_4DIGITS_CIP2016
  )
}

if (
  !dbExistsTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_L_CIP_2DIGITS_CIP2016")
  )
) {
  INFOWARE_L_CIP_2DIGITS_CIP2016 <- dbReadTable(
    iw_con,
    DBI::Id(schema = "INFOWARE", table = "L_CIP_2DIGITS_CIP2016")
  )

  dbWriteTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_L_CIP_2DIGITS_CIP2016"),
    INFOWARE_L_CIP_2DIGITS_CIP2016
  )
}


# ---- Read in INFOWARE tables ----
# iw_config <- config::get("infoware")
# jdbc_config <- config::get("jdbc")

# jdbcDriver <- JDBC(jdbc_config$class, classPath = jdbc_config$path)

# iw_con <- dbConnect(
#   jdbcDriver,
#   iw_config$database,
#   iw_config$uid,
#   iw_config$pwd
# )
# new way to load data from infoware
# iw_config <- config::get("infoware")

# odbcListDrivers()

# iw_con <- dbConnect(
#   odbc::odbc(),
#   Driver = "Oracle in instantclient_19_30",
#   DBQ = "DEV01.world",
#   UID = iw_config$uid,
#   PWD = iw_config$pwd
# )
# all the comment-outed data steps are only running for once.
# now infoware has INFOWARE.L_CIP_6DIGITS_CIP2021 table. We may need to update soon.

if (
  !dbExistsTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_PROGRAMS")
  )
) {
  INFOWARE_PROGRAMS <- dbReadTable(iw_con, DBI::SQL("INFOWARE.PROGRAMS"))
  dbWriteTable(
    con,
    SQL(glue::glue('"{my_schema}"."INFOWARE_PROGRAMS"')),
    INFOWARE_PROGRAMS
  )
}


if (
  !dbExistsTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_PROGRAMS_2016")
  )
) {
  INFOWARE_PROGRAMS_2016 <- dbReadTable(
    iw_con,
    DBI::SQL("INFOWARE.PROGRAMS_BKUP_NOV_2024_CIP2016_CIP2021")
  )
  dbWriteTable(
    con,
    SQL(glue::glue('"{my_schema}"."INFOWARE_PROGRAMS_2016"')),
    INFOWARE_PROGRAMS_2016
  )
}


if (
  !dbExistsTable(
    con,
    DBI::Id(schema = my_schema, table = "INFOWARE_PROGRAMS_HIST_PRGMID_XREF")
  )
) {
  INFOWARE_PROGRAMS_HIST_PRGMID_XREF <- dbReadTable(
    iw_con,
    DBI::SQL("INFOWARE.PROGRAMS_HIST_PRGMID_XREF")
  )
  dbWriteTable(
    con,
    SQL(glue::glue('"{my_schema}"."INFOWARE_PROGRAMS_HIST_PRGMID_XREF"')),
    INFOWARE_PROGRAMS_HIST_PRGMID_XREF
  )
}

