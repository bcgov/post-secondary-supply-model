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

# Workflow #4 (noting here for now)
# Enrolment Analysis
# Description:
# Relies on STP_Enrolment data table, STP_Enrolment_Record_Type, Credential View, AgeGroupLookup
# Creates tables qry09c_MinEnrolment (one of them) to be used for grad projections

library(tidyverse)
set.seed(123456)

# ---- Extract first-time enrolled records ----
# qry01a through qry01e
na_vals = c("", " ", "(Unspecified)")
min_enrolment_sup_var <- stp_enrolment |>
  select(ID, psi_birthdate_cleaned, PSI_MIN_START_DATE) |>
  inner_join(
    stp_enrolment_record_type |>
      select(ID, MinEnrolment, FirstEnrolment, RecordStatus),
    by = "ID"
  ) |>
  rename_with(toupper) |>
  mutate(
    PSI_BIRTHDATE_CLEANED_D = if_else(
      PSI_BIRTHDATE_CLEANED %in%
        na_vals |
        PSI_BIRTHDATE_CLEANED == as.Date("1900-01-01"),
      as.Date(NA),
      as.Date(PSI_BIRTHDATE_CLEANED)
    ),
    PSI_MIN_START_DATE_D = if_else(
      PSI_MIN_START_DATE %in% na_vals,
      as.Date(NA),
      as.Date(PSI_MIN_START_DATE),
    ),
    IS_FIRST_ENROLMENT = if_else(FIRSTENROLMENT == 1, "Yes", NA_character_),
    AGE_AT_ENROL_DATE = NA_real_,
    AGE_GROUP_ENROL_DATE = NA_real_,
    AGE_AT_CENSUS_2016 = NA_real_,
    AGE_GROUP_CENSUS_2016 = NA_real_,
    IS_SKILLS_BASED = NA_integer_
  )

# ---- Create MinEnrolment View ---
min_enrolment <- stp_enrolment |>
  select(
    ID,
    PSI_PEN,
    PSI_BIRTHDATE,
    psi_birthdate_cleaned,
    PSI_GENDER,
    PSI_STUDENT_NUMBER,
    PSI_STUDENT_POSTAL_CODE_FIRST_CONTACT,
    TRUE_PEN,
    ENCRYPTED_TRUE_PEN,
    PSI_SCHOOL_YEAR,
    PSI_REGISTRATION_TERM,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_INDIGENOUS_STATUS,
    PSI_NEW_STUDENT_FLAG,
    PSI_ENROLMENT_SEQUENCE,
    PSI_CODE,
    PSI_TYPE,
    PSI_FULL_NAME,
    PSI_BASIS_OF_ADMISSION,
    PSI_MIN_START_DATE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_PROGRAM_CODE,
    PSI_CIP_CODE,
    PSI_PROGRAM_EFFECTIVE_DATE,
    PSI_FACULTY,
    PSI_CONTINUING_EDUCATION_COURSE_ONLY,
    PSI_CREDENTIAL_CATEGORY,
    PSI_VISA_STATUS,
    PSI_STUDY_LEVEL,
    PSI_ENTRY_STATUS,
    OVERALL_INDIGENOUS_STATUS
  ) |>
  inner_join(
    stp_enrolment_record_type |>
      filter(RecordStatus == 0, MinEnrolment == 1) |>
      select(ID), # We only need the ID to facilitate the filter-join
    by = "ID"
  ) |>
  inner_join(
    min_enrolment_sup_var |>
      select(
        ID,
        PSI_BIRTHDATE_CLEANED_D,
        PSI_MIN_START_DATE_D,
        AGE_AT_ENROL_DATE,
        AGE_GROUP_ENROL_DATE,
        AGE_AT_CENSUS_2016,
        AGE_GROUP_CENSUS_2016,
        IS_FIRST_ENROLMENT,
        IS_SKILLS_BASED
      ),
    by = "ID"
  )

# calculate age at enrolement
min_enrolment <- min_enrolment |>
  # ---- qry02a_UpdateAgeAtEnrol ----
  mutate(
    AGE_AT_ENROL_DATE = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED_D) & !is.na(PSI_MIN_START_DATE_D),
      floor(interval(PSI_BIRTHDATE_CLEANED_D, PSI_MIN_START_DATE_D) / years(1)),
      NA_integer_
    )
  ) |>
  # ---- qry02b_UpdateAGAtEnrol ----
  left_join(
    age_group_lookup,
    by = join_by(between(AGE_AT_ENROL_DATE, LowerBound, UpperBound))
  ) |>
  mutate(AGE_GROUP_ENROL_DATE = AgeIndex) |>
  select(-AgeIndex, -AgeGroup, -LowerBound, -UpperBound)

# assign gender to min enrolement
# Note: there are some epens with > 1 gender still (in the SQL version)
# the original UPDATE query appears to not be deterministic, so choice of gender for
# thes duplicates is arbitrary.
# choosing slice(1)
credential_epen <- credential |>
  filter(!ENCRYPTED_TRUE_PEN %in% na_vals, !psi_gender_cleaned %in% na_vals) |>
  select(ENCRYPTED_TRUE_PEN, gender_cred_epen = psi_gender_cleaned) |>
  slice_max(
    by = ENCRYPTED_TRUE_PEN,
    order_by = gender_cred_epen,
    with_ties = FALSE
  )


# no dups here
credential_no_epen <- credential |>
  filter(ENCRYPTED_TRUE_PEN %in% na_vals, !psi_gender_cleaned %in% na_vals) |>
  select(
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    gender_cred_no_epen = psi_gender_cleaned
  ) |>
  slice_max(
    by = c(PSI_STUDENT_NUMBER, PSI_CODE),
    order_by = gender_cred_no_epen,
    with_ties = FALSE
  )

min_enrolment <- min_enrolment |>
  # ---- qry04a_UpdateMinEnrolment_Gender ----
  left_join(credential_epen, by = join_by(ENCRYPTED_TRUE_PEN)) |> # some duplicates being introduced here
  left_join(credential_no_epen, by = join_by(PSI_STUDENT_NUMBER, PSI_CODE)) |>
  mutate(
    gender_cred = coalesce(gender_cred_epen, gender_cred_no_epen)
  ) |>
  mutate(
    PSI_GENDER = case_when(
      is.na(PSI_GENDER) ~ gender_cred,
      is.na(gender_cred) ~ PSI_GENDER,
      TRUE ~ if_else(PSI_GENDER != gender_cred, gender_cred, PSI_GENDER)
    )
  )


# ---- Find gender for distinct non-null EPENs, or non-null PSI_CODE/PSI_NUMBER  ----
# create a table with unique gender-epen or gender-{psi_code/psi_student_number}
dbExecute(con, qry04b1_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b2_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b3_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b4_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b5_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b6_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b7_tmp_MinEnrolment_Gender)

# sanity check - count of NULL records on concatenated ID variable
dbGetQuery(
  con,
  "SELECT * FROM tmp_MinEnrolment_EPEN_Gender
          WHERE CONCATENATED_ID IS NULL OR CONCATENATED_ID = '';"
)

dbExecute(
  con,
  glue::glue(
    "DROP TABLE [{my_schema}].tmp_MinEnrolment_STUDNUM_PSICODE_Gender_step1;"
  )
)

sql <- SQL(glue::glue(
  'SELECT ID
  , PSI_GENDER
      ,psi_birthdate_cleaned
      ,PSI_MIN_START_DATE
      ,AGE_AT_ENROL_DATE
      ,AGE_GROUP_ENROL_DATE FROM "{my_schema}"."MinEnrolment"
  ORDER BY ID'
))
st <- dbGetQuery(con, sql)
rt <- min_enrolment2 |>
  select(
    ID,
    PSI_GENDER,
    psi_birthdate_cleaned,
    PSI_MIN_START_DATE,
    AGE_AT_ENROL_DATE,
    AGE_GROUP_ENROL_DATE
  ) |>
  distinct()

glimpse(rt)
glimpse(st)

names(rt) <- toupper(names(rt))
names(st) <- toupper(names(st))

i = c(1:6)
anti_join(st[, i], rt) |> nrow()

# ---- Assign one gender/student and update MinEnrolment table ----
# Using Concatenated_ID instead of EPEN for the next set of queries.
dbExecute(con, qry04c_tmp_MinEnrolment_GenderDups)
dbExecute(con, qry04d1_tmp_MinEnrolment_GenderDups_PickGender)
dbExecute(con, qry04d2_tmp_MinEnrolment_GenderDups_PickGender)
dbExecute(con, qry04d3_tmp_MinEnrolment_GenderDups_PickGender)
dbExecute(con, qry04d4_tmp_MinEnrolment_GenderDups_PickGender)
dbExecute(con, qry04d5_tmp_MinEnrolment_GenderDups_PickGender)
dbExecute(con, qry04d6_tmp_MinEnrolment_GenderDups_PickGender)

dbExecute(con, qry04e1_UpdateMinEnrolment_EPEN_GenderDups)
dbExecute(con, qry04e2_UpdateMinEnrolment_EPEN_GenderDups)

dbExecute(
  con,
  glue::glue("DROP TABLE [{my_schema}].tmp_MinEnrolment_EPEN_Gender_step1;")
)
dbExecute(
  con,
  glue::glue("DROP TABLE [{my_schema}].tmp_MinEnrolment_EPEN_Gender;")
)
dbExecute(
  con,
  glue::glue("DROP TABLE [{my_schema}].tmp_Dup_MinEnrolment_EPEN_Gender;")
)
dbExecute(
  con,
  glue::glue(
    "DROP TABLE [{my_schema}].tmp_Dup_MinEnrolment_EPEN_Gender_Unknowns;"
  )
)

# ---- impute gender  ----
# impute gender into records associated with unknown/blank/NULL gender
# this has been done in an Excel worksheet but am moving to code here
# Development\SQL Server\CredentialAnalysis\AgeGenderDistribution (2017)

dbExecute(con, qry05a1_Extract_No_Gender)
dbExecute(con, qry05a1_Extract_No_Gender_First_Enrolment)

# do first enrolments seperatly
PropDist <- dbGetQuery(con, qry05a2_Show_Gender_Distribution)
n <- nrow(dbGetQuery(con, "SELECT * FROM  Extract_No_Gender_First_Enrolment"))
PropDist %>% mutate(p = NumEnrolled / sum(NumEnrolled), top_n = round(p * n))

# for now, enter top_n into query definition (Female into qry06a1, Male into qry06a2)
dbExecute(con, qry06a1_Assign_TopID_Gender)
dbExecute(con, qry06a2_Assign_TopID_Gender2)
dbExecute(con, qry06a2_Assign_TopID_Gender3)

# fill in some of the rest of those not first enrolments
dbExecute(con, qry06a3_CorrectGender1)

# now get N for rest of not matched
n <- nrow(dbGetQuery(
  con,
  "SELECT * FROM  Extract_No_Gender 
WHERE     PSI_GENDER IS NULL 
OR        PSI_GENDER = ' ' 
OR        PSI_GENDER = 'U'
OR        PSI_GENDER = 'Unknown'
OR        PSI_GENDER = '(Unspecified)'"
))

#enter top_n into query definitions that follow
PropDist %>% mutate(p = NumEnrolled / sum(NumEnrolled), top_n = round(p * n))
dbExecute(con, qry06a3_CorrectGender2)
dbExecute(con, qry06a3_CorrectGender3)
dbExecute(con, qry06a3_CorrectGender4)

# not used
# dbExecute(con, glue::glue("DROP TABLE [{my_schema}].GenderDistribution;"))

dbExecute(con, qry06a4a_ExtractNoGender_DupEPENS)
dbExecute(con, qry06a4b_ExtractNoGender_DupEPENS_1)

# update topN again with following numbers
n <- nrow(dbGetQuery(con, "SELECT * FROM  tmp_Extract_No_Gender_DupEPENS"))
PropDist %>% mutate(p = NumEnrolled / sum(NumEnrolled), top_n = round(p * n))

dbExecute(
  con,
  "ALTER TABLE tmp_Extract_No_Gender_DupEPENS ADD PSI_GENDER_to_use VARCHAR(50);"
)
dbExecute(con, qry06a4b_ExtractNoGender_DupEPENS_2)

dbExecute(con, qry06a4c_Update_ExtractNoGender_DupEPENS)
dbExecute(con, qry06a5_CorrectGender2)

# Double check the proportions after assigning gender:
dbGetQuery(con, qry06a4c_Check_Prop)

dbExecute(
  con,
  glue::glue("DROP TABLE [{my_schema}].Extract_No_Gender_First_Enrolment;")
)
dbExecute(
  con,
  glue::glue("DROP TABLE [{my_schema}].tmp_Extract_No_Gender_EPENS;")
)
dbExecute(
  con,
  glue::glue("DROP TABLE [{my_schema}].tmp_Extract_No_Gender_DupEPENS;")
)

# Checks to implement
# remaining PSI_STUDENT_NUMBER and PSI_CODE assigned > 1 gender in table ExtractNoGender.
# null EPEN but PSI_STUDENT_NUMBER/PSI_CODE assigned > 1 gender in view MinEnrolment
# supposedly code for fixing in qry06a4c_Update_ExtractNoGender_DupPSI_Code_Number?

# ---- Create Age and Gender Distrbutions ----
dbExecute(con, qry07a_Extract_No_Age)
dbExecute(con, qry07b_Extract_No_Age_First_Enrolment)
dbExecute(con, "ALTER TABLE Extract_No_Age ADD IS_FIRST_ENROLMENT NVARCHAR(50)")
dbExecute(con, qry07b2_update_Extract_No_Age_IsFirstEnrolment)

# ----- Assign age to records with missing age -----
# impute based on age and gender distribution
extract_no_age_first_enrolment <- dbGetQuery(
  con,
  "SELECT * FROM Extract_No_Age_First_Enrolment"
)
age_dist = dbGetQuery(con, qry07c_Show_Age_Distribution)
n_miss = dbGetQuery(
  con,
  "SELECT PSI_GENDER, COUNT(*) AS n_miss 
                    FROM Extract_No_Age_First_Enrolment 
                    GROUP BY PSI_GENDER"
)

age_dist <- age_dist %>%
  group_by(PSI_GENDER) %>%
  mutate(PropEnrolled = round(NumEnrolled / sum(NumEnrolled), 5)) %>%
  ungroup() %>%
  inner_join(n_miss, by = join_by(PSI_GENDER)) %>%
  mutate(NumDistribution = round(PropEnrolled * n_miss)) %>%
  select(-n_miss)

dbWriteTable(con, name = "AgeDistributionbyGender", age_dist, overwrite = TRUE)

m_id <- extract_no_age_first_enrolment %>%
  filter(PSI_GENDER == 'Male', is.na(AGE_AT_ENROL_DATE)) %>%
  pull(id)

f_id <- extract_no_age_first_enrolment %>%
  filter(PSI_GENDER == 'Female', is.na(AGE_AT_ENROL_DATE)) %>%
  pull(id)

gd_id <- extract_no_age_first_enrolment %>%
  filter(PSI_GENDER == 'Gender Diverse', is.na(AGE_AT_ENROL_DATE)) %>%
  pull(id)

m_dist <- age_dist %>% filter(NumDistribution > 0, PSI_GENDER == 'Male')
f_dist <- age_dist %>% filter(NumDistribution > 0, PSI_GENDER == 'Female')
gd_dist <- age_dist %>%
  filter(NumDistribution > 0, PSI_GENDER == 'Gender Diverse')

m = data.frame(
  id = m_id,
  AGE_AT_ENROL_DATE = sample(
    m_dist$AGE_AT_ENROL_DATE,
    size = length(m_id),
    replace = TRUE,
    prob = m_dist$PropEnrolled
  )
)
f = data.frame(
  id = f_id,
  AGE_AT_ENROL_DATE = sample(
    f_dist$AGE_AT_ENROL_DATE,
    size = length(f_id),
    replace = TRUE,
    prob = f_dist$PropEnrolled
  )
)
# this errors as the gd groups are too small
gd = data.frame(
  id = gd_id,
  AGE_AT_ENROL_DATE = sample(
    gd_dist$AGE_AT_ENROL_DATE,
    size = length(gd_id),
    replace = TRUE,
    prob = gd_dist$PropEnrolled
  )
)

# at this point stop including gd in the distributions
extract_no_age_first_enrolment <- extract_no_age_first_enrolment %>%
  left_join(rbind(m, f), by = join_by(id), suffix = c("", ".new")) %>%
  mutate(
    AGE_AT_ENROL_DATE = if_else(
      is.na(AGE_AT_ENROL_DATE),
      AGE_AT_ENROL_DATE.new,
      AGE_AT_ENROL_DATE
    )
  ) %>%
  select(-AGE_AT_ENROL_DATE.new)

# however this leaves the GD ids with no age, so sample and set them equal to some other age from available
no_age <- extract_no_age_first_enrolment %>%
  filter(is.na(AGE_AT_ENROL_DATE)) %>%
  pull(id)
gd <- data.frame(
  id = no_age,
  AGE_AT_ENROL_DATE = sample(
    extract_no_age_first_enrolment %>%
      filter(!is.na(AGE_AT_ENROL_DATE)) %>%
      pull(AGE_AT_ENROL_DATE),
    size = length(no_age)
  )
)

extract_no_age_first_enrolment <- extract_no_age_first_enrolment %>%
  left_join(gd, by = join_by(id), suffix = c("", ".new")) %>%
  mutate(
    AGE_AT_ENROL_DATE = if_else(
      is.na(AGE_AT_ENROL_DATE),
      AGE_AT_ENROL_DATE.new,
      AGE_AT_ENROL_DATE
    )
  ) %>%
  select(-AGE_AT_ENROL_DATE.new)


dbWriteTable(
  con,
  name = "Extract_No_Age_First_Enrolment",
  value = extract_no_age_first_enrolment,
  overwrite = TRUE
)

dbExecute(con, qry07d1_Update_Extract_No_Age)

# calculate missing ages from first enrolments
multiple_enrol <- dbGetQuery(con, qry02a_Multiple_Enrol)
calc_ages <- dbGetQuery(con, qry02b_Calc_Ages)

for (i in 1:nrow(multiple_enrol)) {
  sn <- multiple_enrol %>% slice(i) %>% pull(PSI_STUDENT_NUMBER)
  code <- multiple_enrol %>% slice(i) %>% pull(PSI_CODE)
  rs <- calc_ages %>%
    filter(PSI_STUDENT_NUMBER == sn, PSI_CODE == code) %>%
    select(
      PSI_STUDENT_NUMBER,
      PSI_CODE,
      PSI_MIN_START_DATE_D,
      AGE_AT_ENROL_DATE
    )
  if (!is.na(rs %>% slice(1) %>% pull(AGE_AT_ENROL_DATE))) {
    date1 = as.POSIXlt(rs[1, "PSI_MIN_START_DATE_D"])
    age1 = rs[1, "AGE_AT_ENROL_DATE"]
    rs[1, "AGE_AT_ENROL_DATE_NEW"] = rs[1, "AGE_AT_ENROL_DATE"]
    for (j in 2:nrow(rs)) {
      date2 = as.POSIXlt(rs[j, "PSI_MIN_START_DATE_D"])
      rs[j, "AGE_AT_ENROL_DATE_NEW"] = age1 + (date2$year - date1$year)
    }
    calc_ages <- left_join(
      calc_ages,
      rs,
      by = join_by(
        PSI_STUDENT_NUMBER,
        PSI_CODE,
        PSI_MIN_START_DATE_D,
        AGE_AT_ENROL_DATE
      )
    ) %>%
      mutate(
        AGE_AT_ENROL_DATE = if_else(
          is.na(AGE_AT_ENROL_DATE),
          AGE_AT_ENROL_DATE_NEW,
          AGE_AT_ENROL_DATE
        )
      ) %>%
      select(-AGE_AT_ENROL_DATE_NEW)
  }
}

calc_ages <- calc_ages %>% select(ID, AGE_AT_ENROL_DATE)
dbWriteTable(con, "R_Extract_No_Age", calc_ages, overwrite = TRUE)
dbExecute(con, qry_Update_Linked_dbo_Extract_No_Age_after_mod2)

# ---- some manual edits ----
dbExecute(con, qry07d_Create_Age_Manual_Fixes_View)
# I think some manual updates to be made here to a handful of records.
# I haven't done the manual fixes as we're getting away from manual work
# Come back to this later.  A different query is in documentation so compare

dbExecute(con, qry07d2_Update_Birthdate)
dbExecute(con, qry07d3_Update_Age)
dbExecute(con, qry07e_Update_MinEnrolment_With_Age)

dbExecute(
  con,
  glue::glue("DROP VIEW [{my_schema}].qry05c_Age_Manual_Fixes_View;")
)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].R_Extract_No_Age;"))
dbExecute(
  con,
  glue::glue("DROP TABLE [{my_schema}].Extract_No_Age_First_Enrolment;")
)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].Extract_No_Age;"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].Extract_No_Gender;"))

dbExecute(con, qry08_UpdateAGAtEnrol)

# ---- Final Distributions ----
dbExecute(con, qry09c_MinEnrolment_by_Credential_and_CIP_Code)
dbExecute(con, qry09c_MinEnrolment_Domestic)
dbExecute(con, qry09c_MinEnrolment)

## Review ----
##I get an error here - invalid object name 'PSI_CODE_RECODE'
# is this another table I need to bring in?
# dbExecute(con, qry09c_MinEnrolment_PSI_TYPE)

# ---- Clean Up ----

# ---- Keep ----
dbExistsTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Credential_Record_Type;"'))
)
dbExistsTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type;"'))
)
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment;"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid;"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential;"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."MinEnrolment"')))

dbDisconnect(con)
