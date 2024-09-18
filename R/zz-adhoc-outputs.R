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