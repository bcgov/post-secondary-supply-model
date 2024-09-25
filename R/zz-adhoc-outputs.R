# ******************************************************************************
# This script is for updating adhoc outputs 
# ******************************************************************************

# ---- Rollup NOCs ----
## ---- libraries and global variables
library(tidyverse)

## ---- Configure LAN Paths ----
lan <- config::get("lan")
folder_path <- glue::glue("{lan}/reports-final/drafts/draft_releases_2021_noc/")
input_draft_file <- glue::glue(folder_path,"public_release_static_no_ptib.csv")

## ---- Read draft data file ----
draft_data <- read_csv(input_draft_file)

## ---- Get lookups ----
T_NOC_Broad_Categories <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_NOC_Broad_Categories_Updated.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

## ---- Create sub NOCs ----
data <- draft_data %>% 
  mutate(NOC_1 = ifelse(NOC=="99998","N/A",str_sub(NOC,1,1)),
         NOC_2 = ifelse(NOC=="99998","N/A",str_sub(NOC,1,2)),
         NOC_3 = ifelse(NOC=="99998","N/A",str_sub(NOC,1,3)),
         NOC_4 = ifelse(NOC=="99998","N/A",str_sub(NOC,1,4))) %>% 
  select(-starts_with("Current_Region"))

## ---- Get results by NOC digit ----
### 1 digit ----
data_1_NOC <- data %>% 
  left_join(T_NOC_Broad_Categories %>%
              mutate(BROAD_CATEGORY_CODE=as.character(BROAD_CATEGORY_CODE)) %>%
              select(BROAD_CATEGORY_CODE,BROAD_CATEGORY_ENGLISH_NAME) %>% distinct(),by=c("NOC_1"="BROAD_CATEGORY_CODE")) %>%
  group_by(Age_Group_Rollup_Label,NOC_1,BROAD_CATEGORY_ENGLISH_NAME) %>% 
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>% ungroup()

write_csv(data_1_NOC,glue::glue(folder_path,"public_release_static_no_ptib_1_digit_rollup.csv"))

### 2 digits ----
data_2_NOC <- data %>% 
  left_join(T_NOC_Broad_Categories %>%
              select(MAJOR_GROUP_CODE,MAJOR_GROUP_ENGLISH_NAME) %>% distinct(),by=c("NOC_2"="MAJOR_GROUP_CODE")) %>%
  group_by(Age_Group_Rollup_Label,NOC_2,MAJOR_GROUP_ENGLISH_NAME) %>% 
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>% ungroup()

write_csv(data_2_NOC,glue::glue(folder_path,"public_release_static_no_ptib_2_digit_rollup.csv"))

# ******************************************************************************
# ---- Rollup NOCs custom groupings ----
## ---- libraries and global variables
library(RODBC)
library(arrow)
library(tidyverse)
library(odbc)
library(RJDBC) ## loads DBI

# Setup
## ---- Configure LAN and file paths ----
lan <- config::get("lan")
my_schema <- config::get("myschema")
folder_path <- glue::glue("{lan}/development/work/adhoc-outputs/")

## ---- Connection to database ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

## ---- Read draft data files ----
public_data <-  tbl(decimal_con, dbplyr::in_schema(sql('"IDIR\\ALOWERY"'), "qry_10a_Model_Public_Release_Union")) %>% collect()
tbl_model <- tbl(decimal_con, dbplyr::in_schema(sql('"IDIR\\ALOWERY"'), "tmp_tbl_Model")) %>% collect()

## ---- Read lookup file ----
noc_rollup <- read_csv(glue::glue(folder_path,"brett_custom_noc_rollup.csv"))

## Public data ----
### ---- Create sub NOCs public data ----
public_data_prep <- public_data %>% 
  mutate(NOC_2 = ifelse(NOC=="99998","N/A",str_sub(NOC,1,2))) %>% 
  left_join(noc_rollup, by="NOC_2") %>% 
  select(-starts_with("Current_Region"),-Expr1,-ENGLISH_NAME,-NOC)

### ---- Get results by custom 2-digit NOC rollup ----
public_data_custom_NOC <- public_data_prep %>% 
  group_by(Age_Group_Rollup_Label,NOC_Custom_Group) %>% 
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>% ungroup()

write_csv(public_data_custom_NOC,glue::glue(folder_path,"public_data_custom_NOC_rollup_20240925.csv"))

## Full Model ----
### ---- Create sub NOCs full model data ----
model_data_prep <- tbl_model %>% 
  filter(Current_Region_PSSM_Code_Rollup==5900) %>% 
  # mutate(NOC_level = str_length(NOC)) %>% 
  # filter(NOC_level==2) %>% 
  filter(NOC_Level==2) %>% 
  left_join(noc_rollup, by=c("NOC"="NOC_2")) %>% 
  select(-starts_with("Current_Region"),-Expr1,-ENGLISH_NAME,-NOC,-NOC_Level)

### ---- Get results by custom 2-digit NOC rollup ----
model_data_custom_NOC <- model_data_prep %>% 
  group_by(Age_Group_Rollup_Label,NOC_Custom_Group) %>% 
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>% ungroup()

write_csv(public_data_custom_NOC,glue::glue(folder_path,"full_model_data_custom_NOC_rollup_20240925.csv"))

## ---- Clean up ----
dbDisconnect(decimal_con)
rm(list=ls())

# ******************************************************************************