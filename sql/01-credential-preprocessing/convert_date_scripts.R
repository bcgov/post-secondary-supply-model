
qrydates_create_tmp_table <- "SELECT CREDENTIAL_AWARD_DATE
      ,PSI_PROGRAM_EFFECTIVE_DATE
      ,ID
INTO tmp_ConvertDateFormatCredential
FROM STP_Credential;"

qrydates_add_cols <- "
  ALTER TABLE tmp_ConvertDateFormatCredential
  ADD CREDENTIAL_AWARD_DATE_convert varchar(50), 
      PSI_PROGRAM_EFFECTIVE_DATE_convert varchar(50);"

qrydates_convert1 <- "UPDATE Tmp_ConvertDateFormatCredential SET Tmp_ConvertDateFormatCredential.CREDENTIAL_AWARD_DATE_convert = '20'+CREDENTIAL_AWARD_DATE
WHERE ((Left(CREDENTIAL_AWARD_DATE,2)<24));"

qrydates_convert2 <- "UPDATE Tmp_ConvertDateFormatCredential SET Tmp_ConvertDateFormatCredential.CREDENTIAL_AWARD_DATE_convert = '19'+CREDENTIAL_AWARD_DATE
WHERE ((Left(CREDENTIAL_AWARD_DATE,2)>23));"

qrydates_convert3 <- "UPDATE Tmp_ConvertDateFormatCredential SET Tmp_ConvertDateFormatCredential.CREDENTIAL_AWARD_DATE_convert = ''
WHERE ((Left(CREDENTIAL_AWARD_DATE,2)='  '));"

qrydates_convert4 <- "UPDATE Tmp_ConvertDateFormatCredential SET Tmp_ConvertDateFormatCredential.PSI_PROGRAM_EFFECTIVE_DATE_convert = '20'+PSI_PROGRAM_EFFECTIVE_DATE
WHERE ((Left(PSI_PROGRAM_EFFECTIVE_DATE,2)<24));"

qrydates_convert5 <- "UPDATE Tmp_ConvertDateFormatCredential SET Tmp_ConvertDateFormatCredential.PSI_PROGRAM_EFFECTIVE_DATE_convert = '19'+PSI_PROGRAM_EFFECTIVE_DATE
WHERE ((Left(PSI_PROGRAM_EFFECTIVE_DATE,2)>23));"

qrydates_convert6 <- "UPDATE Tmp_ConvertDateFormatCredential SET Tmp_ConvertDateFormatCredential.PSI_PROGRAM_EFFECTIVE_DATE_convert = ''
WHERE ((Left(PSI_PROGRAM_EFFECTIVE_DATE,2)='  '));"

qrydates_update_stp_credential1 <- 
"UPDATE STP_Credential
SET STP_Credential.CREDENTIAL_AWARD_DATE = Tmp_ConvertDateFormatCredential.CREDENTIAL_AWARD_DATE_convert
FROM Tmp_ConvertDateFormatCredential, STP_Credential
WHERE STP_Credential.ID = Tmp_ConvertDateFormatCredential.ID;"

qrydates_update_stp_credential2 <- 
"UPDATE STP_Credential
SET STP_Credential.PSI_PROGRAM_EFFECTIVE_DATE = Tmp_ConvertDateFormatCredential.PSI_PROGRAM_EFFECTIVE_DATE_convert
FROM Tmp_ConvertDateFormatCredential, STP_Credential
WHERE STP_Credential.ID = Tmp_ConvertDateFormatCredential.ID;"

qrydates_check1 <- "SELECT ID, CREDENTIAL_AWARD_DATE, PSI_PROGRAM_EFFECTIVE_DATE
  FROM STP_Credential
  where STP_Credential.ID < 100;"

qrydates_check2 <- "SELECT ID, CREDENTIAL_AWARD_DATE, PSI_PROGRAM_EFFECTIVE_DATE
  FROM Tmp_ConvertDateFormatCredential
  where Tmp_ConvertDateFormatCredential.ID < 100;"

