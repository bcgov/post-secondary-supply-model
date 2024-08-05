# ******************************************************************************
# Load PTIB enrolment data from staging area in LAN project folder, to decimal.  
# Raw data is in excel so some cleaning req'd to handle cip code conversion issues
# ******************************************************************************
library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(config)
library(readxl)
library(janitor)

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
raw_data_file <- glue::glue("{lan}/data/ptib/PTIB 2021 and 2022 Enrolment Data for BC Stats 2024.05.31.xlsx")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Read raw data  ----
raw_data <- read_xlsx(raw_data_file, sheet = 1, skip = 2)

# ---- Clean data ----
cleaned_data <- raw_data %>% 
  clean_names() %>% 
  rename(year = calendar_year,
         credential = credential_6,
         graduates = credential_8) %>% ## column is total_enrolments - enrolments_not_graduated
  mutate(cip1 = str_sub(cip, end = 2) %>%
           str_remove_all("\\.") %>% 
           str_pad(width = "2", side = "left", pad = "0"),
         cip2 = ifelse(!is.na(str_extract(cip, "(\\.[:digit:]*)+")),
                       str_extract(cip, "(\\.[:digit:]*)+"),
                       0) %>%
           str_replace_all("(\\.[:digit:]*)\\.", "\\1") %>%
           as.numeric() %>%
           round_half_up(digits = 4) %>%
           as.character() %>%
           str_remove_all("^0\\.") %>%
           str_pad(width = 4, side = "right", pad = "0"),
         cip3 = paste(cip1, cip2, sep = "."))

# ---- Aggregate data ----
data <- cleaned_data %>%
  group_by(year, credential, cip3, age_group, immigration_status) %>%
  summarize(sum_of_graduates = sum(graduates, na.rm = TRUE),
            sum_of_enrolments = sum(enrolments_not_graduated, na.rm = TRUE),
            sum_of_total_enrolments = sum(total_enrolments, na.rm = TRUE),
            .groups = "drop") %>%
  rename(cip = cip3)

# ---- Read LAN data ----
## Lookups
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping"')), T_PSSM_Credential_Grouping)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."T_PTIB_Y1_to_Y10"')), T_PTIB_Y1_to_Y10)

## Last cycle's data for testing - these will be deleted
dbWriteTable(con, "T_Private_Institutions_Credentials_Imported_2021-03", T_Private_Institutions_Credentials_Imported_2021_03)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"')), Graduate_Projections)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')), Cohort_Program_Distributions_Static)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')), Cohort_Program_Distributions_Projected)


# ---- Write to decimal ----
dbWriteTableArrow(con,name = "PTIB_Credentials", nanoarrow::as_nanoarrow_array_stream(data))
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping"')), T_PSSM_Credential_Grouping)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."T_PTIB_Y1_to_Y10"')), T_PTIB_Y1_to_Y10)

dbWriteTable(con, SQL(glue::glue('"{my_schema}"."T_Private_Institutions_Credentials_Imported_2021-03"')), T_Private_Institutions_Credentials_Imported_2021_03)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"')), Graduate_Projections)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')), Cohort_Program_Distributions_Static)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')), Cohort_Program_Distributions_Projected)

# ---- Disconnect ----
dbDisconnect(con)
