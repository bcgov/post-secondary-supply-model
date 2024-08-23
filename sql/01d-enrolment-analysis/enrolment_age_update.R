
qry_Update_Linked_dbo_Extract_No_Age_after_mod2 <- 
"UPDATE dbo_Extract_No_Age INNER JOIN Extract_No_Age ON dbo_Extract_No_Age.id = Extract_No_Age.id SET dbo_Extract_No_Age.AGE_AT_ENROL_DATE = [Extract_No_Age]![AGE_AT_ENROL_DATE]
WHERE (((dbo_Extract_No_Age.AGE_AT_ENROL_DATE) Is Null) AND ((Extract_No_Age.AGE_AT_ENROL_DATE) Is Not Null));"

qry02a_Multiple_Enrol <- 
"SELECT Extract_No_Age.PSI_STUDENT_NUMBER, Extract_No_Age.PSI_CODE, Count(*) AS Expr1
FROM Extract_No_Age
GROUP BY Extract_No_Age.PSI_STUDENT_NUMBER, Extract_No_Age.PSI_CODE
HAVING (((Count(*))>1));"

qry02b_Calc_Ages <- 
"SELECT Extract_No_Age.PSI_STUDENT_NUMBER, Extract_No_Age.PSI_CODE, Extract_No_Age.PSI_SCHOOL_YEAR, Extract_No_Age.PSI_MIN_START_DATE_D, Extract_No_Age.AGE_AT_ENROL_DATE, Extract_No_Age.IS_FIRST_ENROLMENT
FROM Extract_No_Age
ORDER BY Extract_No_Age.PSI_STUDENT_NUMBER, Extract_No_Age.PSI_CODE, Extract_No_Age.PSI_MIN_START_DATE_D, Extract_No_Age.IS_FIRST_ENROLMENT DESC;"

qry03a_Manual_Fixes_ePEN <- 
"SELECT dbo_MinEnrolment.id, Query1.ENCRYPTED_TRUE_PEN, dbo_MinEnrolment.PSI_CODE, dbo_MinEnrolment.PSI_STUDENT_NUMBER, dbo_MinEnrolment.PSI_SCHOOL_YEAR, dbo_MinEnrolment.PSI_MIN_START_DATE_D, dbo_MinEnrolment.PSI_BIRTHDATE_D AS Expr2, dbo_MinEnrolment.AGE_AT_ENROL_DATE, dbo_Extract_No_Age.AGE_AT_ENROL_DATE INTO tmp_tbl_Age_Manual_Fixes
FROM (dbo_MinEnrolment INNER JOIN Query1 ON dbo_MinEnrolment.ENCRYPTED_TRUE_PEN = Query1.ENCRYPTED_TRUE_PEN) INNER JOIN dbo_Extract_No_Age ON dbo_MinEnrolment.ENCRYPTED_TRUE_PEN = dbo_Extract_No_Age.ENCRYPTED_TRUE_PEN
ORDER BY Query1.ENCRYPTED_TRUE_PEN, dbo_MinEnrolment.PSI_MIN_START_DATE_D;"

qry03b_Update_BIRTHDATE <- 
"UPDATE Old_tmp_tbl_Age_Manual_Fixes AS tmp_tbl_Age_Manual_Fixes_1 INNER JOIN Old_tmp_tbl_Age_Manual_Fixes ON tmp_tbl_Age_Manual_Fixes_1.ENCRYPTED_TRUE_PEN = Old_tmp_tbl_Age_Manual_Fixes.ENCRYPTED_TRUE_PEN SET tmp_tbl_Age_Manual_Fixes_1.PSI_BIRTHDATE_D = [Old_tmp_tbl_Age_Manual_Fixes].PSI_BIRTHDATE_D
WHERE (((tmp_tbl_Age_Manual_Fixes_1.PSI_BIRTHDATE_D) Is Null) AND ((Old_tmp_tbl_Age_Manual_Fixes.PSI_BIRTHDATE_D) Is Not Null));"

qry03c_Update_Age <- 
"UPDATE tmp_tbl_Age_Manual_Fixes SET tmp_tbl_Age_Manual_Fixes.dbo_MinEnrolment_AGE_AT_ENROL_DATE = DateDiff('yyyy',[PSI_BIRTHDATE_D],[PSI_MIN_START_DATE_D])+([PSI_MIN_START_DATE_D]<DateSerial(Year([PSI_MIN_START_DATE_D]),Month([PSI_BIRTHDATE_D]),Day([PSI_BIRTHDATE_D])))
WHERE (((tmp_tbl_Age_Manual_Fixes.dbo_Extract_No_Age_AGE_AT_ENROL_DATE) Is Null));"

qry04d_Update_dbo_Extract_No_Age <- 
"UPDATE dbo_Extract_No_Age INNER JOIN Old_tmp_tbl_Age_Manual_Fixes ON dbo_Extract_No_Age.id = Old_tmp_tbl_Age_Manual_Fixes.id SET dbo_Extract_No_Age.AGE_AT_ENROL_DATE = [Old_tmp_tbl_Age_Manual_Fixes].dbo_MinEnrolment_AGE_AT_ENROL_DATE;"

qry05c_Update_Birthdate <- 
"UPDATE dbo_qry05c_Age_Manual_Fixes_View AS dbo_qry05c_Age_Manual_Fixes_View_1 INNER JOIN dbo_qry05c_Age_Manual_Fixes_View ON (dbo_qry05c_Age_Manual_Fixes_View_1.PSI_STUDENT_NUMBER = dbo_qry05c_Age_Manual_Fixes_View.PSI_STUDENT_NUMBER) AND (dbo_qry05c_Age_Manual_Fixes_View_1.PSI_CODE = dbo_qry05c_Age_Manual_Fixes_View.PSI_CODE) SET dbo_qry05c_Age_Manual_Fixes_View.PSI_BIRTHDATE_D = [dbo_qry05c_Age_Manual_Fixes_View_1].[PSI_BIRTHDATE_D]
WHERE ((([dbo_qry05c_Age_Manual_Fixes_View].[PSI_BIRTHDATE_D]) Is Null) AND (([dbo_qry05c_Age_Manual_Fixes_View_1].[PSI_BIRTHDATE_D]) Is Not Null));"

Check_AdvancedAge_ForEPENS <- 
  "SELECT Extract_No_Age.id, Extract_No_Age.ENCRYPTED_TRUE_PEN, Extract_No_Age.PSI_STUDENT_NUMBER, Extract_No_Age.PSI_CODE, Extract_No_Age.AGE_AT_ENROL_DATE, Extract_No_Age.PSI_SCHOOL_YEAR, Extract_No_Age.PSI_MIN_START_DATE, Extract_No_Age.PSI_MIN_START_DATE_D, Extract_No_Age.IS_FIRST_ENROLMENT
FROM Extract_No_Age
WHERE (((Extract_No_Age.ENCRYPTED_TRUE_PEN)<>''))
ORDER BY Extract_No_Age.ENCRYPTED_TRUE_PEN, Extract_No_Age.PSI_SCHOOL_YEAR;"

Check_AdvancedAge_ForStudentNumPSICODE <- 
  "SELECT Extract_No_Age.id, Extract_No_Age.ENCRYPTED_TRUE_PEN, Extract_No_Age.PSI_STUDENT_NUMBER, Extract_No_Age.PSI_CODE, Extract_No_Age.AGE_AT_ENROL_DATE, Extract_No_Age.PSI_SCHOOL_YEAR, Extract_No_Age.PSI_MIN_START_DATE, Extract_No_Age.PSI_MIN_START_DATE_D, Extract_No_Age.IS_FIRST_ENROLMENT
FROM Extract_No_Age
WHERE (((Extract_No_Age.ENCRYPTED_TRUE_PEN)=''))
ORDER BY Extract_No_Age.PSI_STUDENT_NUMBER, Extract_No_Age.PSI_CODE, Extract_No_Age.PSI_SCHOOL_YEAR;"
