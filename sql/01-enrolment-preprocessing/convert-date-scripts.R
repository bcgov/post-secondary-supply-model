# ---- qrydates_create_tmp_table ----
qrydates_create_tmp_table <- "
SELECT ID, 
       PSI_BIRTHDATE, 
       LAST_SEEN_BIRTHDATE, 
       PSI_MIN_START_DATE, 
       PSI_PROGRAM_EFFECTIVE_DATE
  INTO tmp_ConvertDateFormat
  FROM STP_Enrolment;"

# ---- qrydates_add_cols ----
qrydates_add_cols <- "
ALTER TABLE tmp_ConvertDateFormat
  ADD PSI_BIRTHDATE_convert varchar(50), 
      LAST_SEEN_BIRTHDATE_convert varchar(50),
      PSI_MIN_START_DATE_convert varchar(50),
      PSI_PROGRAM_EFFECTIVE_DATE_convert varchar(50);"

# ---- qrydates_convert1 ----
qrydates_convert1 <- "
UPDATE Tmp_ConvertDateFormat 
SET Tmp_ConvertDateFormat.PSI_BIRTHDATE_CONVERT = '20'+PSI_BIRTHDATE
WHERE ((Left(PSI_BIRTHDATE,2)<24));"

# ---- qrydates_convert2 ----
qrydates_convert2 <- "
UPDATE Tmp_ConvertDateFormat 
SET Tmp_ConvertDateFormat.PSI_BIRTHDATE_CONVERT = '19'+PSI_BIRTHDATE
WHERE ((Left(PSI_BIRTHDATE,2)>23));"

# ---- qrydates_convert3 ----
qrydates_convert3 <- "
UPDATE Tmp_ConvertDateFormat 
SET Tmp_ConvertDateFormat.PSI_BIRTHDATE_CONVERT = ''
WHERE ((Left(PSI_BIRTHDATE,2)='  '));"

# ---- qrydates_convert4 ----
qrydates_convert4 <- "
UPDATE Tmp_ConvertDateFormat 
SET Tmp_ConvertDateFormat.LAST_SEEN_BIRTHDATE_CONVERT = '20'+LAST_SEEN_BIRTHDATE
WHERE ((Left(LAST_SEEN_BIRTHDATE,2)<24));"

# ---- qrydates_convert5 ----
qrydates_convert5 <- "
UPDATE Tmp_ConvertDateFormat 
SET Tmp_ConvertDateFormat.LAST_SEEN_BIRTHDATE_CONVERT = '19'+LAST_SEEN_BIRTHDATE
WHERE ((Left(LAST_SEEN_BIRTHDATE,2)>23));"

# ---- qrydates_convert6 ----
qrydates_convert6 <- "
UPDATE Tmp_ConvertDateFormat 
SET Tmp_ConvertDateFormat.LAST_SEEN_BIRTHDATE_CONVERT = ''
WHERE ((Left(LAST_SEEN_BIRTHDATE,2)='  '));"

# ---- qrydates_convert7 ----
qrydates_convert7 <- "UPDATE Tmp_ConvertDateFormat 
SET Tmp_ConvertDateFormat.PSI_MIN_START_DATE_CONVERT = '20'+PSI_MIN_START_DATE
WHERE ((Left(PSI_MIN_START_DATE,2)<24));"

# ---- qrydates_convert8 ----
qrydates_convert8 <- "
UPDATE Tmp_ConvertDateFormat 
SET Tmp_ConvertDateFormat.PSI_MIN_START_DATE_CONVERT = '19'+PSI_MIN_START_DATE
WHERE ((Left(PSI_MIN_START_DATE,2)>23));"

# ---- qrydates_convert9 ----
qrydates_convert9 <- "
UPDATE Tmp_ConvertDateFormat 
SET Tmp_ConvertDateFormat.PSI_MIN_START_DATE_CONVERT = ''
WHERE ((Left(PSI_MIN_START_DATE,2)='  '));"

# ---- qrydates_convert10 ----
qrydates_convert10 <- "UPDATE Tmp_ConvertDateFormat SET Tmp_ConvertDateFormat.PSI_PROGRAM_EFFECTIVE_DATE_CONVERT = '20'+PSI_PROGRAM_EFFECTIVE_DATE
WHERE ((Left(PSI_PROGRAM_EFFECTIVE_DATE,2)<24));"

# ---- qrydates_convert11 ----
qrydates_convert11 <- "
UPDATE Tmp_ConvertDateFormat 
SET Tmp_ConvertDateFormat.PSI_PROGRAM_EFFECTIVE_DATE_CONVERT = '19'+PSI_PROGRAM_EFFECTIVE_DATE
WHERE ((Left(PSI_PROGRAM_EFFECTIVE_DATE,2)>23));"

# ---- qrydates_convert12 ----
qrydates_convert12 <- "
UPDATE Tmp_ConvertDateFormat 
SET Tmp_ConvertDateFormat.PSI_PROGRAM_EFFECTIVE_DATE_CONVERT = ''
WHERE ((Left(PSI_PROGRAM_EFFECTIVE_DATE,2)='  '));"

# ---- qrydates_update1 ----
qrydates_update1 <- "
UPDATE STP_Enrolment
SET STP_Enrolment.PSI_BIRTHDATE = tmp_ConvertDateFormat.PSI_BIRTHDATE_CONVERT
FROM tmp_ConvertDateFormat, STP_Enrolment
WHERE STP_Enrolment.ID = tmp_ConvertDateFormat.ID;"

# ---- qrydates_update2 ----
qrydates_update2 <- "
UPDATE STP_Enrolment
SET STP_Enrolment.LAST_SEEN_BIRTHDATE = tmp_ConvertDateFormat.LAST_SEEN_BIRTHDATE_CONVERT
FROM tmp_ConvertDateFormat, STP_Enrolment
WHERE STP_Enrolment.ID = tmp_ConvertDateFormat.ID;"

# ---- qrydates_update3 ----
qrydates_update3 <- "
UPDATE STP_Enrolment
SET STP_Enrolment.PSI_MIN_START_DATE = tmp_ConvertDateFormat.PSI_MIN_START_DATE_CONVERT
FROM tmp_ConvertDateFormat, STP_Enrolment
WHERE STP_Enrolment.ID = tmp_ConvertDateFormat.ID;"

# ---- qrydates_update4 ----
qrydates_update4 <- "
UPDATE STP_Enrolment
SET STP_Enrolment.PSI_PROGRAM_EFFECTIVE_DATE = tmp_ConvertDateFormat.PSI_PROGRAM_EFFECTIVE_DATE_CONVERT
FROM tmp_ConvertDateFormat, STP_Enrolment
WHERE STP_Enrolment.ID = tmp_ConvertDateFormat.ID;"
