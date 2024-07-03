library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
source(glue::glue("{lan}/development/sql/gh-source/01c-enrolment-preprocessing/pssm-birthdate-cleaning.R"))

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

# Birthdate data into it's own temp table. 
dbExecute(con, qry01_BirthdateCleaning)

# Find the records with >1 birth date 
dbExecute(con, qry02_BirthdateCleaning)

dbExecute(con, "ALTER TABLE tmp_Birthdate ADD psi_birthdate_cleaned VARCHAR(19)")
dbExecute(con, "ALTER TABLE STP_Enrolment ADD psi_birthdate_cleaned VARCHAR(19)")
dbExecute(con, "ALTER TABLE tmp_MoreThanOne_Birthdate ADD psi_birthdate_cleaned VARCHAR(19)")

# Find the min non-null birthdate for each EPEN 
dbExecute(con, qry03_BirthdateCleaning)
dbExecute(con, "ALTER TABLE tmp_MinPSIBirthdate ADD NumBirthdateRecords INT")

# Find the max non-null birthdate for each EPEN 
dbExecute(con, qry04_BirthdateCleaning)
dbExecute(con, "ALTER TABLE tmp_MaxPSIBirthdate ADD NumBirthdateRecords INT")

# Run to update the NumBirthdateRecords in the tmp_MinPSIBirthdate  
dbExecute(con, qry05_BirthdateCleaning)

# run to update the NumBirthdateRecords in the tmp_MaxPSIBirthdate 
dbExecute(con, qry06_BirthdateCleaning)
dbExecute(con, "ALTER TABLE tmp_MoreThanOne_Birthdate 
          ADD MinPSIBirthdate VARCHAR (19),
          NumMinBirthdateRecords INT,
          MaxPSIBirthdate VARCHAR(19),
          NumMaxBirthdateRecords INT")

# update the tmp_MoreThanOne_Birthdate table columns for the min bdtates
dbExecute(con, qry07a_BirthdateCleaning)

# update the tmp_MoreThanOne_Birthdate table columns for the max bdtates  
dbExecute(con, qry07b_BirthdateCleaning)

dbExecute(con, "ALTER TABLE tmp_MoreThanOne_Birthdate ADD LastSeenBirthdate VARCHAR(19)")
dbExecute(con, qry08_BirthdateCleaning)

dbExecute(con, "ALTER TABLE tmp_MoreThanOne_Birthdate 
          ADD MaxOrMin_MostCommon VARCHAR(50),
          Year_Max INT,
          Year_Min INT,
          YearDif_MaxMinusMin INT 
          UseMaxOrMin_FINAL VARCHAR(50), 
          MaxOrMin_MostCommon VARCHAR(50)")

dbExecute(con, "UPDATE tmp_MoreThanOne_Birthdate 
          SET Year_Min = YEAR(MinPSIBirthdate),
          Year_Max = YEAR(MaxPSIBirthdate), 
          YearDif_MaxMinusMin = YEAR(MaxPSIBirthdate) - YEAR(MinPSIBirthdate)")

# ---- Manual Work ----
# decided whether to use max or min birthdate based on the frequency of birthdate year. I think we can pull this into R and take the mode
tmp_Clean_MaxMinBirthDate <- dbReadTable(con, "tmp_MoreThanOne_Birthdate")
readr::write_csv(glue::glue("{lan}/development/csv/gh-source/01c-clean-max-min-birthdate.csv"))
tmp_Clean_MaxMinBirthDate <- readr::read_csv(glue::glue("{lan}/development/csv/gh-source/01c-clean-max-min-birthdate.csv"), col_types = cols(.default = col_character()))
dbWriteTable(con, name = "tmp_Clean_MaxMinBirthDate", tmp_Clean_MaxMinBirthDate)

# Update these new columns in the tmp_MoreThanOne_Birthdate 
dbExecute(con, qry09_BirthdateCleaning)

# set psi_birthdate_cleaned with the min non-null birthdate if min most common
dbExecute(con, qry10_BirthdateCleaning)

# set psi_birthdate_cleaned with the max non-null birthdate if max most common
dbExecute(con, qry11_BirthdateCleaning)

# update the psi_birthdate_cleaned in STP_enrolment
dbExecute(con, qry12_BirthdateCleaning)

# Now deal with the EPENS that have one null and one non-null birthdate
dbExecute(con, qry13_BirthdateCleaning)

# Make a table to EPENS and their non-null birthdates 
dbExecute(con, qry14_BirthdateCleaning)

# Make a table with the EPENS and that had non-null and a null birthdate
dbExecute(con, qry15_BirthdateCleaning)

#add a column called psi_birthdate_cleaned (varchar(19) to tmp_NullBirthdateCleaned 
dbExecute(con, "ALTER TABLE tmp_NullBirthdateCleaned ADD psi_birthdate_cleaned varchar(19)")

# Populate the cleaned birthdate with the min birthdate for EPENs with multi-birthdates
dbExecute(con, qry16_BirthdateCleaning)

# for the remaining records that need a null birthdate filled in use the PSI_BIRTHDATE
dbExecute(con, qry17_BirthdateCleaning)

# Update the STP_Enrolment table for the records with a null birthdate 
dbExecute(con, qry18_BirthdateCleaning)

# the remaining records are the ones that have only one PSI_BIRTHDATE so we can just copy that date over to to PSI_birthdate cleaned.
dbExecute(con, qry19_BirthdateCleaning)

# Now find if there is a record with a null EPEN and null birthdate but a corresponding PSI_STUDENT_NUMBER/PSI_CODE table with a birthdate.
dbExecute(con, qry20_BirthdateCleaning)

# Finds any PSI_STUDENT_NUMBER/PSI_CODE combos associated with >1 birthdate
dbExecute(con, qry21_BirthdateCleaning)

dbExecute(con , "DROP TABLE tmp_BirthDate")
dbExecute(con , "DROP TABLE tmp_Clean_MaxMinBirthDate")
dbExecute(con , "DROP TABLE tmp_MaxPSIBirthdate")
dbExecute(con , "DROP TABLE tmp_MinPSIBirthdate")
dbExecute(con , "DROP TABLE tmp_MoreThanOne_Birthdate")

dbExecute(con , "DROP TABLE tmp_NullBirthDate")
dbExecute(con , "DROP TABLE tmp_NonNullBirthDate")
dbExecute(con , "DROP TABLE tmp_NullBirthDateCleaned")
dbExecute(con , "DROP TABLE tmp_TEST_multi_birthdate")

