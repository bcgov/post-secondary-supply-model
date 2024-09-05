# This script prepares census data for the Labour_Supply_Distribution table
#
# Required tables
#   filtered and unfiltered stat can exports
#   tbl_age_groups_rollup
#   t_current_region_pssm_rollup_codes_statcan
#   t_region_statcan_xwalk
#
# Resulting tables
#   Labour_Supply_Distribution_Stat_Can


library(tidyverse)
library(openxlsx)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Import stat can data ----
options(scipen=999)
stat_can_export <- glue::glue("{lan}/data/statcan/stat-can-data-export-for-labour-supply-distributions.xlsx")

## Fix column headers ----
# TOT = total_labour_force_status, LF = "in_labour_force", LF_E = "employed", LF_U = "unemployed")
# tot_sa = "total_school_attendance", dnas = "did_not_attend_school", as = "attended_school")
VAR_status <- c("tot_sa", "dnas", "as")

cols_data <- c("age_group", "HCDD", "geography", "major_field_cip", paste0("TOT_", VAR_status), 
               paste0("LF_", VAR_status), paste0("LF_E_", VAR_status), paste0("LF_U_", VAR_status))

count_cols <- c(paste0("TOT_", VAR_status), 
              paste0("LF_", VAR_status), paste0("LF_E_", VAR_status), paste0("LF_U_", VAR_status))


sc_export_unfilt_orig <- read.xlsx(stat_can_export, sheet = "unfiltered_data", startRow = 5)
names(sc_export_unfilt_orig) <- cols_data
sc_export_filt_orig   <- read.xlsx(stat_can_export, sheet = "filtered_data",   startRow = 5)
names(sc_export_filt_orig) <- cols_data

## Clean values ----
sc_export_filt_orig <- sc_export_filt_orig %>%
  mutate(age_group = str_trim(age_group), HCDD = str_trim(HCDD),major_field_cip=str_trim(major_field_cip))

sc_export_unfilt_orig <- sc_export_unfilt_orig %>%
  mutate(age_group = str_trim(age_group), HCDD = str_trim(HCDD),major_field_cip=str_trim(major_field_cip))

# ---- Import required lookups ----
t_current_region_pssm_rollup_codes_statcan <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes_StatCan.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps") 

t_region_statcan_xwalk <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Region_StatCan_XWALK.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps") %>% 
  mutate(RAW_STAT_CAN_NAME = str_replace(RAW_STAT_CAN_NAME,"\x96","–"))
  
dbWriteTable(decimal_con, name = "t_current_region_pssm_rollup_codes_statcan", value = t_current_region_pssm_rollup_codes_statcan)

# ---- Check for required data tables ----
# lookups
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_age_groups_rollup"')))

# ---- Create required Region counts ----
## use xwalk to get "clean" names
sc_export_filt <- sc_export_filt_orig %>% 
  left_join(t_region_statcan_xwalk,by=c("geography"="RAW_STAT_CAN_NAME")) %>% 
  select(-geography,REGION=FIXED_NAME)

sc_export_unfilt <- sc_export_unfilt_orig %>% 
  left_join(t_region_statcan_xwalk,by=c("geography"="RAW_STAT_CAN_NAME")) %>% 
  select(-geography,REGION=FIXED_NAME)

## Create Northeast ----
# Take "North Coast - Nechako and Northeast" and remove "North Coast and Nechako" to get Northeast
# start with filtered data; repeated for unfiltered - add to respective data
Northeast_filt <- sc_export_filt %>%
  filter(REGION %in% c("North Coast - Nechako and Northeast", "North Coast and Nechako")) %>%
  pivot_longer(-c("age_group", "HCDD", "major_field_cip", "REGION"), names_to = "variable", values_to = "value" ) %>%
  pivot_wider(names_from = "REGION", values_from = "value") %>%
  mutate(qry_Northeast = `North Coast - Nechako and Northeast` - `North Coast and Nechako`) %>%
  select(-`North Coast - Nechako and Northeast`, -`North Coast and Nechako`) %>%
  pivot_wider(names_from = "variable", values_from = "qry_Northeast") %>%
  mutate(REGION = "qry_Northeast")

sc_export_filt <- sc_export_filt %>% bind_rows(Northeast_filt)

Northeast_unfilt <- sc_export_unfilt %>%
  filter(REGION %in% c("North Coast - Nechako and Northeast", "North Coast and Nechako")) %>%
  pivot_longer(-c("age_group", "HCDD", "major_field_cip", "REGION"), names_to = "variable", values_to = "value" ) %>%
  pivot_wider(names_from = "REGION", values_from = "value") %>%
  mutate(qry_Northeast = `North Coast - Nechako and Northeast` - `North Coast and Nechako`) %>%
  select(-`North Coast - Nechako and Northeast`, -`North Coast and Nechako`) %>%
  pivot_wider(names_from = "variable", values_from = "qry_Northeast") %>%
  mutate(REGION = "qry_Northeast")

sc_export_unfilt <- sc_export_unfilt %>% bind_rows(Northeast_unfilt)
## Create Rest of Canada counts ----
# Take "Canada" and remove "British Columbia" to get Rest of Canada
# start with filtered data; repeated for unfiltered - add to respective data
Rest_of_Canada_filt <- sc_export_filt %>%
  filter(REGION %in% c("British Columbia", "Canada")) %>%
  pivot_longer(-c("age_group", "HCDD", "major_field_cip", "REGION"), names_to = "variable", values_to = "value" ) %>%
  pivot_wider(names_from = "REGION", values_from = "value") %>%
  mutate(qry_Rest_of_Canada = Canada - `British Columbia`) %>%
  select(-Canada, -`British Columbia`) %>%
  pivot_wider(names_from = "variable", values_from = "qry_Rest_of_Canada") %>%
  mutate(REGION = "qry_Rest_of_Canada")

sc_export_filt <- sc_export_filt %>% bind_rows(Rest_of_Canada_filt)

Rest_of_Canada_unfilt <- sc_export_unfilt %>%
  filter(REGION %in% c("British Columbia", "Canada")) %>%
  pivot_longer(-c("age_group", "HCDD", "major_field_cip", "REGION"), names_to = "variable", values_to = "value" ) %>%
  pivot_wider(names_from = "REGION", values_from = "value") %>%
  mutate(qry_Rest_of_Canada = Canada - `British Columbia`) %>%
  select(-Canada, -`British Columbia`) %>%
  pivot_wider(names_from = "variable", values_from = "qry_Rest_of_Canada") %>%
  mutate(REGION = "qry_Rest_of_Canada")

sc_export_unfilt <- sc_export_unfilt %>% bind_rows(Rest_of_Canada_unfilt)

# ---- Update data for each designation separately ----

## GRCT or GRDP (uses filtered data) ----
grct_grdp_data <- sc_export_filt %>% 
  filter(HCDD == "University certificate or diploma above bachelor level")

# get Canada totals by age_group and cip
grct_grdp_canada <- grct_grdp_data %>% 
  filter(REGION=="Canada") %>% 
  select(age_group,major_field_cip,TOT_tot_sa_Canada=TOT_tot_sa)

# append Canada totals; run required calculations
grct_grdp_data <- grct_grdp_data %>% 
  left_join(grct_grdp_canada,by=c("age_group","major_field_cip")) %>% 
  mutate(LABOUR_SUPPLY = ifelse((LF_U_dnas + LF_E_tot_sa)>TOT_tot_sa,TOT_tot_sa,(LF_U_dnas + LF_E_tot_sa))) %>% 
  mutate(NEW_LABOUR_SUPPLY = ifelse(TOT_tot_sa_Canada==0,0,LABOUR_SUPPLY/TOT_tot_sa_Canada)) %>% 
  mutate(TOTAL = ifelse(LABOUR_SUPPLY==0,0,TOT_tot_sa_Canada)) %>% 
  mutate(PSSM_CREDENTIAL = "GRCT or GRDP",
         PSSM_CRED = PSSM_CREDENTIAL)

## PDEG (uses filtered data) ----
pdeg_data <- sc_export_filt %>% 
  filter(HCDD == "Degree in medicine, dentistry, veterinary medicine or optometry")

# get Canada totals by age_group and cip
pdeg_canada <- pdeg_data %>% 
  filter(REGION=="Canada") %>% 
  select(age_group,major_field_cip,TOT_tot_sa_Canada=TOT_tot_sa)

# append Canada totals; run required calculations
pdeg_data <- pdeg_data %>% 
  left_join(pdeg_canada,by=c("age_group","major_field_cip")) %>% 
  mutate(LABOUR_SUPPLY = ifelse((LF_U_dnas + LF_E_tot_sa)>TOT_tot_sa,TOT_tot_sa,(LF_U_dnas + LF_E_tot_sa))) %>% 
  mutate(NEW_LABOUR_SUPPLY = ifelse(TOT_tot_sa_Canada==0,0,LABOUR_SUPPLY/TOT_tot_sa_Canada)) %>% 
  mutate(TOTAL = ifelse(LABOUR_SUPPLY==0,0,TOT_tot_sa_Canada)) %>% 
  mutate(PSSM_CREDENTIAL = "PDEG",
         PSSM_CRED = PSSM_CREDENTIAL)

## MAST (uses unfiltered data) ----
mast_data <- sc_export_unfilt %>% 
  filter(HCDD == "Master's degree")

# get Canada totals by age_group and cip
mast_canada <- mast_data %>% 
  filter(REGION=="Canada") %>% 
  select(age_group,major_field_cip,TOT_tot_sa_Canada=TOT_tot_sa)

# append Canada totals; run required calculations
mast_data <- mast_data %>% 
  left_join(mast_canada,by=c("age_group","major_field_cip")) %>% 
  mutate(LABOUR_SUPPLY = ifelse((LF_U_dnas + LF_E_tot_sa)>TOT_tot_sa,TOT_tot_sa,(LF_U_dnas + LF_E_tot_sa))) %>% 
  mutate(NEW_LABOUR_SUPPLY = ifelse(TOT_tot_sa_Canada==0,0,LABOUR_SUPPLY/TOT_tot_sa_Canada)) %>% 
  mutate(TOTAL = ifelse(LABOUR_SUPPLY==0,0,TOT_tot_sa_Canada)) %>% 
  mutate(PSSM_CREDENTIAL = "MAST",
         PSSM_CRED = PSSM_CREDENTIAL)
  
## DOCT (uses unfiltered data) ----
doct_data <- sc_export_unfilt %>% 
  filter(HCDD == "Earned doctorate")

# get Canada totals by age_group and cip
doct_canada <- doct_data %>% 
  filter(REGION=="Canada") %>% 
  select(age_group,major_field_cip,TOT_tot_sa_Canada=TOT_tot_sa)

# get total cip for BC total values
doct_bc_tot <- doct_data %>% 
  filter(grepl("Total",major_field_cip)) %>% 
  filter(REGION=="British Columbia") %>% 
  select(age_group,TOT_tot_sa_bc=TOT_tot_sa)

# get total cip for each region
doct_all_tots <- doct_data %>% 
  filter(grepl("Total",major_field_cip)) %>% 
  select(REGION,age_group,TOT_tot_sa_reg=TOT_tot_sa)

# get BC data; run BC calculations
doct_bc <- doct_data %>% 
  filter(REGION=="British Columbia") %>% 
  left_join(doct_bc_tot,by="age_group") %>% 
  mutate(BC_LABOUR_SUPPLY=ifelse((LF_U_dnas + LF_E_tot_sa)>TOT_tot_sa,TOT_tot_sa,(LF_U_dnas + LF_E_tot_sa))) %>% 
  mutate(BC_NEW_LABOUR_SUPPLY_TEMP = ifelse(TOT_tot_sa==0,0,BC_LABOUR_SUPPLY/TOT_tot_sa))

doct_bc_tot_temp <- doct_bc %>% 
  filter(grepl("Total",major_field_cip)) %>% 
  select(age_group,TOT_BC_NEW_LS = BC_NEW_LABOUR_SUPPLY_TEMP)

# replace labour supply depending on outcomes
doct_bc <- doct_bc %>% 
  left_join(doct_bc_tot_temp,by="age_group") %>% 
  mutate(BC_NEW_LABOUR_SUPPLY = case_when((BC_NEW_LABOUR_SUPPLY_TEMP == 1 & BC_LABOUR_SUPPLY <= 30) ~ TOT_BC_NEW_LS,
                                          BC_LABOUR_SUPPLY==0 ~ TOT_BC_NEW_LS,
                                          TRUE ~ BC_NEW_LABOUR_SUPPLY_TEMP)) %>% 
  mutate(LS_Program_Dist_BC = (BC_NEW_LABOUR_SUPPLY*TOT_tot_sa)/TOT_tot_sa_bc) %>% 
  select(age_group,major_field_cip,BC_NEW_LABOUR_SUPPLY,BC_NEW_LABOUR_SUPPLY_TEMP,BC_LABOUR_SUPPLY,TOT_tot_sa_bc,LS_Program_Dist_BC)

# run rest of canada calculations - differ from other regions
doct_rest_canada <- doct_data %>% 
  filter(REGION=="qry_Rest_of_Canada") %>% 
  left_join(doct_canada,by=c("age_group","major_field_cip")) %>%
  mutate(REST_CAN_LABOUR_SUPPLY=ifelse((LF_U_dnas + LF_E_tot_sa)>TOT_tot_sa,TOT_tot_sa,(LF_U_dnas + LF_E_tot_sa))) %>% 
  mutate(REST_CAN_NEW_LABOUR_SUPPLY_TEMP = ifelse(TOT_tot_sa==0,0,REST_CAN_LABOUR_SUPPLY/TOT_tot_sa)) %>% 
  mutate(REST_CAN_NEW_LABOUR_SUPPLY_TEMP2 = REST_CAN_LABOUR_SUPPLY*REST_CAN_NEW_LABOUR_SUPPLY_TEMP) %>% 
  mutate(REST_CAN_NEW_LABOUR_SUPPLY = ifelse(TOT_tot_sa_Canada==0,0,REST_CAN_NEW_LABOUR_SUPPLY_TEMP2/TOT_tot_sa_Canada)) %>% 
  select(REGION,age_group,major_field_cip,REST_CAN_LABOUR_SUPPLY,REST_CAN_NEW_LABOUR_SUPPLY_TEMP2,REST_CAN_NEW_LABOUR_SUPPLY,REST_CAN_NEW_LABOUR_SUPPLY_TEMP)

# append required values; run required calculations
doct_data_final <- doct_data %>%
  left_join(doct_canada,by=c("age_group","major_field_cip")) %>%
  left_join(doct_bc,by=c("age_group","major_field_cip")) %>%
  left_join(doct_rest_canada,by=c("REGION","age_group","major_field_cip")) %>% 
  left_join(doct_all_tots,by=c("REGION","age_group")) %>% 
  mutate(LABOUR_SUPPLY = case_when(REGION=="qry_Rest_of_Canada" ~ REST_CAN_LABOUR_SUPPLY,
                                   REGION=="British Columbia" ~ BC_LABOUR_SUPPLY,
                                   TRUE ~ TOT_tot_sa_reg*LS_Program_Dist_BC)) %>%
  mutate(NEW_LABOUR_SUPPLY = case_when(REGION=="qry_Rest_of_Canada" ~ REST_CAN_NEW_LABOUR_SUPPLY,
                                       REGION=="British Columbia" ~ BC_NEW_LABOUR_SUPPLY,
                                       TRUE ~ (ifelse(TOT_tot_sa_Canada==0,0,LABOUR_SUPPLY/TOT_tot_sa_Canada)))) %>%
  mutate(TOTAL = ifelse(LABOUR_SUPPLY==0,0,TOT_tot_sa_Canada)) %>%
  mutate(PSSM_CREDENTIAL = "DOCT",
         PSSM_CRED = PSSM_CREDENTIAL) %>% 
  select(-contains(c("BC_","REST_CAN_","_bc","_reg")))

# ---- Prepare a Stat_Can version of Labour_Supply_Distribution table ----
## Combine all datasets ----
Combined_Stat_Can_Original <- grct_grdp_data %>% 
  rbind(pdeg_data) %>% 
  rbind(mast_data) %>% 
  rbind(doct_data_final)

## Temporarily save to decimal ----
dbWriteTable(decimal_con, name = "Combined_Labour_Supply_Stat_Can_Original", value = Combined_Stat_Can_Original)

## Add in lookups ----
# filter out unused regions and ages based on lookup tables
Combined_Stat_Can <- tbl(decimal_con,"Combined_Labour_Supply_Stat_Can_Original") %>% 
  left_join(tbl(decimal_con,"t_current_region_pssm_rollup_codes_statcan"),by=c("REGION"="CURRENT_REGION_PSSM_NAME_ROLLUP_STAT_CAN")) %>%
  left_join(tbl(decimal_con,"tbl_age_groups_rollup"),by=c("age_group"="AGE_GROUP_ROLLUP_LABEL")) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLUP)) %>% 
  filter(!is.na(AGE_GROUP_ROLLUP)) %>% 
  collect()

## Prepare final columns ----
Combined_Stat_Can <- Combined_Stat_Can %>% 
  filter(!grepl("Total",major_field_cip)) %>% 
  mutate(LCP4_CD = substr(major_field_cip,1,2)) %>% 
  mutate(LCIP4_CRED = paste0(LCP4_CD," - ",PSSM_CREDENTIAL)) %>% 
  mutate(SURVEY="2021 Census PSSM 2022-2023") %>% 
  arrange(PSSM_CRED,AGE_GROUP_ROLLUP,CURRENT_REGION_PSSM_CODE_ROLLUP,LCP4_CD)

# select desired columns
Labour_Supply_Distribution_Stat_Can <- Combined_Stat_Can %>% 
  select(SURVEY, PSSM_CREDENTIAL,PSSM_CRED, LCP4_CD,LCIP4_CRED,
         CURRENT_REGION_PSSM_CODE_ROLLUP,AGE_GROUP_ROLLUP,COUNT=LABOUR_SUPPLY,TOTAL,NEW_LABOUR_SUPPLY)

## Save final table ----
dbWriteTable(decimal_con, name = "Labour_Supply_Distribution_Stat_Can", Labour_Supply_Distribution_Stat_Can)

# ---- Clean Up ----
## Drop intermediate tables ----
dbExecute(decimal_con, "DROP TABLE Combined_Labour_Supply_Stat_Can_Original")

## Drop lookups ----
dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes_statcan")

## Disconnect ----
dbDisconnect(decimal_con)


