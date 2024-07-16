library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
lan_2019 <- config::get("lan_2019")
source(glue::glue("{lan}/development/sql/gh-source/01d-enrolment-analysis/01d-enrolment-analysis.R"))

db_config <- config::get("decimal")
my_schema <- config::get("myschema")
db_schema <- config::get("dbschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Check Required Tables etc. ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."AgeGroupLookup"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential"')))

# ---- Extract MinEnrolment records and delete Skill Based Suspect records ---- 
dbExecute(con, qry01a_MinEnrolmentSupVar)
dbExecute(con, "ALTER table MinEnrolmentSupVar ADD CONSTRAINT PK_MinEnrolSupVarsID PRIMARY KEY (ID)")
dbExecute(con, qry01b_MinEnrolmentSupVar)
dbExecute(con, qry01c_MinEnrolmentSupVar)
dbExecute(con, qry01d1_MinEnrolmentSupVar)
dbExecute(con, qry01d2_MinEnrolmentSupVar)
dbExecute(con, qry01e_MinEnrolmentSupVar)

# ---- Create MinEnrolment View ---
# created from the STP_Enrolment, STP_Enrolment_Record_Type and MinEnrolmentSupVar tables
dbExecute(con, qry_CreateMinEnrolmentView)

dbExecute(con, qry02a_UpdateAgeAtEnrol)
dbExecute(con, qry02b_UpdateAGAtEnrol) 

# ---- Find gender for distinct non-null EPENs/{CODE_NUMBER}'s  ---- 
dbExecute(con, qry04a1_UpdateMinEnrolment_Gender)
dbExecute(con, qry04a2_UpdateMinEnrolment_Gender)
dbExecute(con, qry04b1_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b2_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b3_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b4_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b5_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b6_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b7_tmp_MinEnrolment_Gender)

# ---- Quick Checks ----
# count of NULL records on concatenated ID variable
dbGetQuery(con, "SELECT * FROM tmp_MinEnrolment_EPEN_Gender
          WHERE CONCATENATED_ID IS NULL OR CONCATENATED_ID = '';")

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].tmp_MinEnrolment_STUDNUM_PSICODE_Gender_step1;"))

# Using Concatenated_ID instead of EPEN for the next set of queries.

# ---- Assign one gender/student ---- 
dbExecute(con, qry04c_tmp_MinEnrolment_GenderDups)
dbExecute(con, qry04d1_tmp_MinEnrolment_GenderDups_PickGender)
dbExecute(con, qry04d2_tmp_MinEnrolment_GenderDups_PickGender)
dbExecute(con, qry04d3_tmp_MinEnrolment_GenderDups_PickGender)
dbExecute(con, qry04d4_tmp_MinEnrolment_GenderDups_PickGender)
dbExecute(con, qry04d5_tmp_MinEnrolment_GenderDups_PickGender)
dbExecute(con, qry04d6_tmp_MinEnrolment_GenderDups_PickGender)
dbExecute(con, qry04d7_tmp_MinEnrolment_GenderDups_PickGender)

dbExecute(con, qry04e1_UpdateMinEnrolment_EPEN_GenderDups)
dbExecute(con, qry04e2_UpdateMinEnrolment_EPEN_GenderDups)

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].tmp_MinEnrolment_EPEN_Gender_step1;"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].tmp_MinEnrolment_EPEN_Gender;"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].tmp_Dup_MinEnrolment_EPEN_Gender;"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].tmp_Dup_MinEnrolment_EPEN_Gender_Unknowns;"))

dbExecute(con, qry05a1_Extract_No_Gender)
dbExecute(con, qry05a1_Extract_No_Gender_First_Enrolment )

# ---- impute gender  ---- 
# impute gender into records associated with unknown/blank/NULL gender
# this is currently done in an Excel worksheet but can be moved to code here
dbExecute(con, qry05a2_Show_Gender_Distribution)
dbExecute(con, qry06a1_Assign_TopID_Gender)
dbExecute(con, qry06a2_Assign_TopID_Gender2)
dbExecute(con, qry06a3_CorrectGender1)
# The next two queries assign a gender to a small subset of records which 
# are not first enrolments. The number of updated records does not match those reported in documentation.  
dbExecute(con, qry06a3_CorrectGender2)
dbExecute(con, qry06a3_CorrectGender3)

# check if these tables exist in the DB?
#dbExecute(con, glue::glue("DROP TABLE [{my_schema}].tmp_MinEnrolment_EPEN_Gender_step1_Check;"))
#dbExecute(con, glue::glue("DROP TABLE [{my_schema}].tmp_Dup_MinEnrolment_EPEN_Gender_CHECK;"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].GenderDistribution;"))

dbExecute(con, qry06a4a_ExtractNoGender_DupEPENS)
dbExecute(con, "ALTER TABLE tmp_Extract_No_Gender_DupEPENS ADD PSI_GENDER_to_use VARCHAR(50);")
dbExecute(con, qry06a4b_ExtractNoGender_DupEPENS_1)
dbExecute(con, qry06a4b_ExtractNoGender_DupEPENS_2)
dbExecute(con, qry06a4c_Update_ExtractNoGender_DupEPENS)

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].Extract_No_Gender_First_Enrolment;"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].tmp_Extract_No_Gender_EPENS;"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].tmp_Extract_No_Gender_DupEPENS;"))

# There may still be records with PSI_STUDENT_NUMBER and PSI_CODE assigned > 1 gender in table ExtractNoGender . 
# Also there may be records null EPEN, but PSI_STUDENT_NUMBER and PSI_CODE assigned > 1 gender in view MinEnrolment
# Code for fixing is in qry06a4c_Update_ExtractNoGender_DupPSI_Code_Number

# Double check the proportions after assigning gender:
dbGetQuery(con, qry06a4c_Check_Prop)
dbExecute(con, qry06a5_CorrectGender2)

# ---- Create Age and Gender Distrbutions ---- 
dbExecute(con, qry07a_Extract_No_Age)
dbExecute(con, qry07b_Extract_No_Age_First_Enrolment)
dbExecute(con, "ALTER TABLE Extract_No_Age ADD IS_FIRST_ENROLMENT NVARCHAR(50)")
dbExecute(con, qry07b2_update_Extract_No_Age_IsFirstEnrolment)

# ----- Assign age to records with missing age -----
# impute based on age and gender distribution
extract_no_age_first_enrolment <- dbGetQuery(con, "SELECT * FROM Extract_No_Age_First_Enrolment")
age_dist = dbGetQuery(con, qry07c_Show_Age_Distribution)
n_miss = dbGetQuery(con, "SELECT PSI_GENDER, COUNT(*) AS n_miss FROM Extract_No_Age_First_Enrolment GROUP BY PSI_GENDER")

age_dist <- age_dist %>% 
  group_by(PSI_GENDER) %>% 
  mutate(PropEnrolled = round(NumEnrolled/sum(NumEnrolled),5)) %>%
  ungroup() %>%
  inner_join(n_miss, by = join_by(PSI_GENDER)) %>%
  mutate(NumDistribution = round(PropEnrolled*n_miss)) %>%
  select(-n_miss)

dbWriteTable(con, name = "AgeDistributionbyGender", age_dist, overwrite = TRUE)

m_id <- extract_no_age_first_enrolment %>% 
  filter(PSI_GENDER =='M', is.na(AGE_AT_ENROL_DATE)) %>%
  pull(id) 

f_id <- extract_no_age_first_enrolment %>% 
  filter(PSI_GENDER =='F', is.na(AGE_AT_ENROL_DATE)) %>%
  pull(id)

m_dist <- age_dist %>% filter(NumDistribution > 0,  PSI_GENDER == 'M')
f_dist <- age_dist %>% filter(NumDistribution > 0,  PSI_GENDER == 'F')

m = data.frame(id = m_id, AGE_AT_ENROL_DATE = sample(m_dist$AGE_AT_ENROL_DATE, size = length(m_id), replace = TRUE, prob = m_dist$PropEnrolled))
f = data.frame(id = f_id, AGE_AT_ENROL_DATE = sample(f_dist$AGE_AT_ENROL_DATE, size = length(f_id), replace = TRUE, prob = f_dist$PropEnrolled))

extract_no_age_first_enrolment <- extract_no_age_first_enrolment %>% 
  left_join(rbind(m,f), by = join_by(id), suffix = c("", ".new"))  %>% 
  mutate(AGE_AT_ENROL_DATE = if_else(is.na(AGE_AT_ENROL_DATE), AGE_AT_ENROL_DATE.new, AGE_AT_ENROL_DATE)) %>%
  select(-AGE_AT_ENROL_DATE.new)

dbWriteTable(con, 
             name = "Extract_No_Age_First_Enrolment", 
             value = extract_no_age_first_enrolment, 
             overwrite = TRUE)


dbExecute(con, qry07d1_Update_Extract_No_Age)

# forward calculate missing ages first enrolments
multiple_enrol <- dbGetQuery(con, qry02a_Multiple_Enrol)
calc_ages <- dbGetQuery(con, qry02b_Calc_Ages)

for (i in 1:nrow(multiple_enrol)){
  sn   <- multiple_enrol %>% slice(i) %>% pull(PSI_STUDENT_NUMBER)
  code <- multiple_enrol %>% slice(i) %>% pull(PSI_CODE)
  rs   <- calc_ages %>% filter(PSI_STUDENT_NUMBER == sn, PSI_CODE == code) %>% select(PSI_STUDENT_NUMBER, PSI_CODE, PSI_MIN_START_DATE_D, AGE_AT_ENROL_DATE)
  if(!is.na(rs %>% slice(1) %>% pull(AGE_AT_ENROL_DATE))) {
    date1 =  as.POSIXlt(rs[1,"PSI_MIN_START_DATE_D"])
    age1 = rs[1,"AGE_AT_ENROL_DATE"]
    rs[1,"AGE_AT_ENROL_DATE_NEW"] = rs[1,"AGE_AT_ENROL_DATE"]
    for (j in 2:nrow(rs)){
      date2 = as.POSIXlt(rs[j,"PSI_MIN_START_DATE_D"])
      rs[j,"AGE_AT_ENROL_DATE_NEW"] = age1 + (date2$year-date1$year)
    }
    calc_ages <- left_join(calc_ages, rs, by = join_by(PSI_STUDENT_NUMBER, PSI_CODE, PSI_MIN_START_DATE_D, AGE_AT_ENROL_DATE)) %>% 
      mutate(AGE_AT_ENROL_DATE = if_else(is.na(AGE_AT_ENROL_DATE), AGE_AT_ENROL_DATE_NEW, AGE_AT_ENROL_DATE)) %>%
      select(-AGE_AT_ENROL_DATE_NEW)
  }
}

calc_ages <- calc_ages %>% select(ID, AGE_AT_ENROL_DATE)
dbWriteTable(con, "R_Extract_No_Age", calc_ages, overwrite = TRUE)
dbExecute(con,qry_Update_Linked_dbo_Extract_No_Age_after_mod2)

# ---- some manual edits ----
dbExecute(con, qry07d_Create_Age_Manual_Fixes_View)
# I think some manual updates to be made here to a handful of records.  
# I haven't done the manual fixes as we're getting away from manual work
# Come back to this later.  A different query is in documentation so compare

dbExecute(con, qry07d2_Update_Birthdate)
dbExecute(con, qry07d3_Update_Age)
dbExecute(con, qry07e_Update_MinEnrolment_With_Age)

dbExecute(con, glue::glue("DROP VIEW [{my_schema}].qry05c_Age_Manual_Fixes_View;"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].R_Extract_No_Age;"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].Extract_No_Age_First_Enrolment;"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].Extract_No_Age;"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].Extract_No_Gender;"))

dbExecute(con, qry08_UpdateAGAtEnrol)

# ---- Final Distributions ----
dbGetQuery(con, qry09c_MinEnrolment_by_Credential_and_CIP_Code)
dbGetQuery(con, qry09c_MinEnrolment_Domestic)
dbGetQuery(con, qry09c_MinEnrolment_PSI_TYPE)

# ---- Clean Up ----
dbExecute(con, glue::glue("DROP VIEW [{my_schema}].MinEnrolment;"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].MinEnrolmentSupVar;"))
dbDisconnect(con)






