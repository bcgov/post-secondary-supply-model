
# ---- ----
qry01_BirthdateCleaning <-  "
SELECT     ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE, count(*) as NumBirthdateRecords
INTO        tmp_BirthDate
FROM        STP_Enrolment
GROUP BY    ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE"

# ---- ----
qry02_BirthdateCleaning <- "
SELECT     ENCRYPTED_TRUE_PEN, COUNT(*) AS N_Birthdates
INTO        tmp_MoreThanOne_Birthdate
FROM        tmp_BirthDate
WHERE       PSI_BIRTHDATE <> ' ' 
  AND       ENCRYPTED_TRUE_PEN <> ' '
GROUP BY    ENCRYPTED_TRUE_PEN
HAVING      COUNT(*) > 1"

# ---- ----
qry03_BirthdateCleaning <- 
"SELECT     ENCRYPTED_TRUE_PEN, MIN(PSI_BIRTHDATE) AS MinPSIBirthdate
INTO        tmp_MinPSIBirthdate
FROM        tmp_BirthDate
WHERE       PSI_BIRTHDATE <> ' '
GROUP BY    ENCRYPTED_TRUE_PEN"

# ---- ----
qry04_BirthdateCleaning <-"
SELECT     ENCRYPTED_TRUE_PEN, MAX(PSI_BIRTHDATE) AS MaxPSIBirthdate
INTO        tmp_MaxPSIBirthdate
FROM        tmp_BirthDate
WHERE       PSI_BIRTHDATE <> ' '
AND         ENCRYPTED_TRUE_PEN <> ' '
GROUP BY    ENCRYPTED_TRUE_PEN"

# ---- ----
qry05_BirthdateCleaning <- 
"UPDATE       tmp_MinPSIBirthdate
SET           NumBirthdateRecords = tmp_BirthDate.NumBirthdateRecords
FROM          tmp_MinPSIBirthdate 
INNER JOIN    tmp_BirthDate 
  ON          tmp_MinPSIBirthdate.ENCRYPTED_TRUE_PEN = tmp_BirthDate.ENCRYPTED_TRUE_PEN 
  AND         tmp_MinPSIBirthdate.MinPSIBirthdate = tmp_BirthDate.PSI_BIRTHDATE"

# ---- ----
qry06_BirthdateCleaning <-  "
UPDATE        tmp_MaxPSIBirthdate
SET           NumBirthdateRecords = tmp_BirthDate.NumBirthdateRecords
FROM          tmp_MaxPSIBirthdate 
INNER JOIN    tmp_BirthDate 
  ON          tmp_MaxPSIBirthdate.ENCRYPTED_TRUE_PEN = tmp_BirthDate.ENCRYPTED_TRUE_PEN 
  AND         tmp_MaxPSIBirthdate.MaxPSIBirthdate = tmp_BirthDate.PSI_BIRTHDATE"

# ---- ----
qry07a_BirthdateCleaning <- "
UPDATE        tmp_MoreThanOne_Birthdate
SET           MinPSIBirthdate = tmp_MinPSIBirthdate.MinPSIBirthdate, 
              NumMinBirthdateRecords = tmp_MinPSIBirthdate.NumBirthdateRecords
FROM          tmp_MoreThanOne_Birthdate 
INNER JOIN    tmp_MinPSIBirthdate 
  ON          tmp_MoreThanOne_Birthdate.ENCRYPTED_TRUE_PEN = tmp_MinPSIBirthdate.ENCRYPTED_TRUE_PEN"

# ---- ----
qry07b_BirthdateCleaning <- "
UPDATE        tmp_MoreThanOne_Birthdate
SET           MaxPSIBirthdate = tmp_MaxPSIBirthdate.MaxPSIBirthdate, 
              NumMaxBirthdateRecords = tmp_MaxPSIBirthdate.NumBirthdateRecords
FROM          tmp_MoreThanOne_Birthdate 
INNER JOIN    tmp_MaxPSIBirthdate 
  ON          tmp_MoreThanOne_Birthdate.ENCRYPTED_TRUE_PEN = tmp_MaxPSIBirthdate.ENCRYPTED_TRUE_PEN"

# ---- ----
qry08_BirthdateCleaning <-  
"UPDATE       tmp_MoreThanOne_Birthdate
SET           LastSeenBirthdate = STP_Enrolment.LAST_SEEN_BIRTHDATE
FROM          tmp_MoreThanOne_Birthdate 
INNER JOIN    STP_Enrolment 
ON            tmp_MoreThanOne_Birthdate.ENCRYPTED_TRUE_PEN = STP_Enrolment.ENCRYPTED_TRUE_PEN"

# ---- ----
qry09_BirthdateCleaning <-  "
UPDATE      tmp_MoreThanOne_Birthdate
SET         UseMaxOrMin_FINAL = CASE 
              WHEN MaxPSIBirthdate = LastSeenBirthdate THEN 'MAX'
              WHEN NumMaxBirthdateRecords > NumMinBirthdateRecords THEN 'MAX'
              WHEN NumMaxBirthdateRecords < NumMinBirthdateRecords THEN 'MIN'
              ELSE 'MIN' END
FROM        tmp_MoreThanOne_Birthdate"

# ---- ----
qry10_BirthdateCleaning <- 
"UPDATE       tmp_MoreThanOne_Birthdate
SET                psi_birthdate_cleaned = MinPSIBirthdate
WHERE        (USEMAXORMIN_FINAL = 'MIN')"

# ---- ----
qry11_BirthdateCleaning <-  
"UPDATE       tmp_MoreThanOne_Birthdate
SET                psi_birthdate_cleaned = MaxPSIBirthdate
WHERE        (USEMAXORMIN_FINAL = 'MAX')"
               
# ---- ----
qry12_BirthdateCleaning <-  
"UPDATE    STP_Enrolment
SET              psi_birthdate_cleaned = tmp_MoreThanOne_Birthdate.psi_birthdate_cleaned
FROM         STP_Enrolment INNER JOIN
                      tmp_MoreThanOne_Birthdate ON STP_Enrolment.ENCRYPTED_TRUE_PEN = tmp_MoreThanOne_Birthdate.ENCRYPTED_TRUE_PEN"
# ---- ----
qry13_BirthdateCleaning <-  
"SELECT     ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE
 INTO            tmp_NullBirthdate
 FROM         tmp_BirthDate
 GROUP BY ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE
HAVING      (ENCRYPTED_TRUE_PEN <> ' ') AND (PSI_BIRTHDATE = ' ')"

# ---- ----
qry14_BirthdateCleaning <- "
 SELECT     ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE
 INTO        tmp_NonNullBirthdate
 FROM       tmp_BirthDate
 GROUP BY ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE
HAVING      (ENCRYPTED_TRUE_PEN <> ' ') AND (PSI_BIRTHDATE <> ' ')"

# ---- ----
qry15_BirthdateCleaning <-
"SELECT     tmp_NonNullBirthdate.ENCRYPTED_TRUE_PEN, tmp_NonNullBirthdate.PSI_BIRTHDATE
INTO        tmp_NullBirthdateCleaned
FROM        tmp_NonNullBirthdate 
INNER JOIN  tmp_NullBirthdate 
  ON tmp_NonNullBirthdate.ENCRYPTED_TRUE_PEN = tmp_NullBirthdate.ENCRYPTED_TRUE_PEN"

# ---- ----
qry16_BirthdateCleaning <- "
UPDATE    tmp_NullBirthdateCleaned
SET       psi_birthdate_cleaned = tmp_MoreThanOne_Birthdate.psi_birthdate_cleaned
FROM      tmp_NullBirthdateCleaned 
INNER JOIN tmp_MoreThanOne_Birthdate 
ON tmp_NullBirthdateCleaned.ENCRYPTED_TRUE_PEN = tmp_MoreThanOne_Birthdate.ENCRYPTED_TRUE_PEN"            

# ---- ----
qry17_BirthdateCleaning <-  
"UPDATE    tmp_NullBirthdateCleaned
SET              psi_birthdate_cleaned = PSI_BIRTHDATE
WHERE     (psi_birthdate_cleaned IS NULL)"

# ---- ----
qry18_BirthdateCleaning <-  "
UPDATE    STP_Enrolment
SET       psi_birthdate_cleaned = tmp_NullBirthdateCleaned.psi_birthdate_cleaned
FROM      STP_Enrolment 
INNER JOIN  tmp_NullBirthdateCleaned 
  ON STP_Enrolment.ENCRYPTED_TRUE_PEN = tmp_NullBirthdateCleaned.ENCRYPTED_TRUE_PEN
WHERE     (STP_Enrolment.psi_birthdate_cleaned IS NULL) 
OR        (STP_Enrolment.psi_birthdate_cleaned = ' ')"

# ---- ----               
qry19_BirthdateCleaning <-  
 "UPDATE    STP_Enrolment
 SET         psi_birthdate_cleaned = PSI_BIRTHDATE
WHERE       (psi_birthdate_cleaned IS NULL) AND (NOT (PSI_BIRTHDATE IS NULL)) 
OR          (psi_birthdate_cleaned = ' ') AND (NOT (PSI_BIRTHDATE IS NULL)) 
OR          (psi_birthdate_cleaned IS NULL) AND (PSI_BIRTHDATE <> ' ') 
OR          (psi_birthdate_cleaned = ' ') AND (PSI_BIRTHDATE <> ' ')"


# ---- ----
qry20_BirthdateCleaning <-  
"SELECT     PSI_STUDENT_NUMBER, PSI_BIRTHDATE, psi_birthdate_cleaned, PSI_CODE, COUNT(*) AS Expr1
INTO            tmp_TEST_multi_birthdate
FROM         STP_Enrolment
WHERE     (ENCRYPTED_TRUE_PEN = ' ')
GROUP BY PSI_STUDENT_NUMBER, PSI_BIRTHDATE, psi_birthdate_cleaned, PSI_CODE
HAVING      (PSI_BIRTHDATE <> ' ') AND (PSI_STUDENT_NUMBER <> ' ') AND (PSI_CODE <> ' ')"

# ---- ----
qry21_BirthdateCleaning <-  
"SELECT     PSI_STUDENT_NUMBER, PSI_CODE, COUNT(*) AS Expr1
FROM         tmp_TEST_multi_birthdate
GROUP BY    PSI_STUDENT_NUMBER, PSI_CODE
HAVING      (COUNT(*) > 1)"

