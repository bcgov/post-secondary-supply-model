library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
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
dbExistsTable(con, SQL(glue::glue('"{db_schema}"."AgeGroupLookup"')))

# ---- Extract MinEnrolment records and delete Skill Based Suspect records ---- 
dbExecute(con, qry01a_MinEnrolmentSupVar)
dbExecute(con, "ALTER table MinEnrolmentSupVar ADD CONSTRAINT PK_MinEnrolSupVarsID PRIMARY KEY (ID)")
dbExecute(con, qry01b_MinEnrolmentSupVar)
dbExecute(con, qry01c_MinEnrolmentSupVar)
dbExecute(con, qry01d1_MinEnrolmentSupVar)
dbExecute(con, qry01d2_MinEnrolmentSupVar)
dbExecute(con, qry01e_MinEnrolmentSupVar)

dbExecute(con, qry_CreateMinEnrolmentView)
dbExecute(con, qry02a_UpdateAgeAtEnrol)
dbExecute(con, qry02b_UpdateAGAtEnrol)

dbExecute(con, qry04b1_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b2_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b3_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b4_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b5_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b6_tmp_MinEnrolment_Gender)
dbExecute(con, qry04b7_tmp_MinEnrolment_Gender)

# ---- Quick Checks ----
# there should be no NULL records on concatenated ID variable
dbGetQuery(con, "SELECT * FROM tmp_MinEnrolment_EPEN_Gender
          WHERE CONCATENATED_ID IS NULL OR CONCATENATED_ID = '';")
dbGetQuery(con, "SELECT TOP 1000 * FROM tmp_MinEnrolment_EPEN_Gender;")

# Now there is a table with all the distinct EPENs and null EPEN but PSI_Student_Number/PSI_Code combos.
# We will use the Concatenated_ID instead of EPEN for the next set of queries.


dbExecute(con, )
dbExecute(con, )
dbExecute(con, )
dbExecute(con, )
dbExecute(con, )


# ---- Clean Up ----
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[STP_Enrolment_Record_Type];"))  
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[STP_Enrolment_Valid];"))   
dbDisconnect(con)






