# ******************************************************************************
# Load PTIB enrolment data from staging area in LAN project folder, to decimal.  
# Raw data is in excel.
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
raw_data_file <- glue::glue("{lan}/data/ptib/PTIB 2021 and 2022 Enrolment Data for BC Stats.xlsx")

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
         age_group = age_range,
         graduates = credential_8) %>% ## column is total_enrolments - enrolments_not_graduated
  ## The cip column reads in wonky from excel and needs to be cleaned.
  ## Start by extracting the 2 digits left of the decimal.
  ## In cases where there is only 1 digit, remove the decimal from the extracted string,
  ## then pad with zeros to the left to make it 2 characters.
  mutate(cip1 = str_sub(cip, end = 2) %>%
           str_remove_all("\\.") %>% 
           str_pad(width = "2", side = "left", pad = "0"),
  ## For digits to the right of the decimal,
  ## extract strings in the format of a decimal followed by numbers, with an optional second decimal.
  ## If there are no decimals in the cip code, set to 0 (as this returns NA).
  ## Remove any second decimals.
  ## Convert the string to numeric and round to fix floating point values,
  ## then change back to character and remove the "0." that was added by converting to numeric.
  ## Finally pad with zeros to the right to make it 4 characters long.
         cip2 = ifelse(!is.na(str_extract(cip, "(\\.[:digit:]*)+")),
                       str_extract(cip, "(\\.[:digit:]*)+"),
                       0) %>%
           str_replace_all("(\\.[:digit:]*)\\.", "\\1") %>%
           as.numeric() %>%
           round_half_up(digits = 4) %>%
           as.character() %>%
           str_remove_all("^0\\.") %>%
           str_pad(width = 4, side = "right", pad = "0"),
  ## Combine left and right cleaned values.
         cip3 = paste(cip1, cip2, sep = "."))

# ---- Aggregate data ----
data <- cleaned_data %>%
  group_by(year, credential, cip3, age_group, immigration_status) %>%
  summarize(sum_of_graduates = sum(graduates, na.rm = TRUE),
            sum_of_enrolments = sum(enrolments_not_graduated, na.rm = TRUE),
            sum_of_total_enrolments = sum(total_enrolments, na.rm = TRUE),
            .groups = "drop") %>%
  rename(cip = cip3)


# ---- Write to decimal ----
dbWriteTableArrow(con,
                  name = "PTIB_Credentials",
                  nanoarrow::as_nanoarrow_array_stream(data))

# ---- Disconnect ----
dbDisconnect(con)

# ---- Testing ----
## dbReadTable(con, "PTIB_Credentials")
## dbRemoveTable(con, "PTIB_Credentials") 
