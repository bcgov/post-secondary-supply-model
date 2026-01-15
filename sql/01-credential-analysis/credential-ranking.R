# ---- qry11a_RankByDateRank ----
qry11a_RankByDateRank <-
  "SELECT id,
credential_ranking.encrypted_true_pen,
credential_ranking.psi_student_number,
credential_ranking.psi_code,
--credential_ranking.concatenated_id,
credential_ranking.credential_award_date_d,
credential_ranking.rank,
credential_ranking.highest_cred_by_date
FROM   credential_ranking
ORDER  BY credential_ranking.encrypted_true_pen,
credential_ranking.psi_student_number,
credential_ranking.psi_code,
credential_ranking.credential_award_date_d DESC,
credential_ranking.rank;"


# ---- qry11b_RankByRankDate ----
qry11b_RankByRankDate <-
  "SELECT 
 id, credential_ranking.encrypted_true_pen,
       credential_ranking.psi_student_number,
       credential_ranking.psi_code,
       --credential_ranking.concatenated_id,
       credential_ranking.rank,
       credential_ranking.credential_award_date_d,
       credential_ranking.highest_cred_by_rank
FROM   credential_ranking
ORDER  BY credential_ranking.encrypted_true_pen,
          credential_ranking.psi_student_number,
          credential_ranking.psi_code,
          credential_ranking.rank,
          credential_ranking.credential_award_date_d DESC;"


# ---- qry18a_ExtrLaterAwarded ----
qry18a_ExtrLaterAwarded <-
  "SELECT DISTINCT credential_non_dup.id  AS LID,
                tblcredential_highestrank.id AS HID,
                credential_non_dup.concatenated_id,
                credential_non_dup.credential_award_date_d AS LATER_AWARD_DATE,
                credential_non_dup.highest_cred_by_date,
                credential_non_dup.psi_award_school_year,
                tblcredential_highestrank.credential_award_date_d AS HIGHEST_AWARD_DATE,
                credential_non_dup.psi_credential_category,
                credentialrank.rank
INTO   tblcredential_laterawarded
FROM   (credential_non_dup
        INNER JOIN credentialrank
                ON credential_non_dup.psi_credential_category =
                   credentialrank.psi_credential_category)
       INNER JOIN tblcredential_highestrank
               ON credential_non_dup.concatenated_id = tblcredential_highestrank.concatenated_id
WHERE  (( ( credential_non_dup.credential_award_date_d ) >
                  [tblcredential_highestrank].[credential_award_date_d]))"

# ---- qry18b_ExtrLaterAwarded ----
qry18b_ExtrLaterAwarded <-
  "SELECT tblCredential_LaterAwarded.LID, 
        tblCredential_LaterAwarded.HID, 
        tblCredential_LaterAwarded.concatenated_id, 
        tblCredential_LaterAwarded.LATER_AWARD_DATE, 
        tblCredential_LaterAwarded.PSI_AWARD_SCHOOL_YEAR
INTO tmp_qry18b_ExtrLaterAwarded
FROM tblCredential_HighestRank 
INNER JOIN tblCredential_LaterAwarded 
ON tblCredential_HighestRank.concatenated_id = tblCredential_LaterAwarded.concatenated_id
WHERE (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='APPRENTICESHIP' 
     Or (tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='BACHELORS DEGREE' 
     Or (tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='FIRST PROFESSIONAL DEGREE')) 
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='ADVANCED DIPLOMA' 
  Or (tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='ADVANCED CERTIFICATE') 
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=36)) 
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='ASSOCIATE DEGREE') 
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=18)) 
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='CERTIFICATE') 
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=18)) 
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='DIPLOMA') 
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=30)) 
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='MASTERS DEGREE') 
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=30)) 
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='GRADUATE CERTIFICATE') 
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=18)) 
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='GRADUATE DIPLOMA') 
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=30)) 
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='POST-DEGREE CERTIFICATE') 
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=18)) 
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='POST-DEGREE DIPLOMA') 
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=30));"

# ---- qry18c_ExtrLaterAwarded ----
qry18c_ExtrLaterAwarded <-
  "SELECT tmp_qry18b_ExtrLaterAwarded.concatenated_id, 
Max(tmp_qry18b_ExtrLaterAwarded.LATER_AWARD_DATE) AS MaxOfLATER_AWARD_DATE
INTO tmp_qry18c_ExtrLaterAwarded
FROM tmp_qry18b_ExtrLaterAwarded
GROUP BY tmp_qry18b_ExtrLaterAwarded.concatenated_id;"

# ---- qry18d_ExtrLaterAwarded ----
qry18d_ExtrLaterAwarded <-
  "SELECT DISTINCT Min(tmp_qry18b_ExtrLaterAwarded.LID) AS LID, 
        tmp_qry18b_ExtrLaterAwarded.HID, 
        tmp_qry18b_ExtrLaterAwarded.concatenated_id, 
        tmp_qry18b_ExtrLaterAwarded.LATER_AWARD_DATE, 
        tmp_qry18b_ExtrLaterAwarded.PSI_AWARD_SCHOOL_YEAR 
INTO    tblCredential_DelayEffect
FROM    tmp_qry18b_ExtrLaterAwarded 
INNER JOIN tmp_qry18c_ExtrLaterAwarded 
  ON    tmp_qry18b_ExtrLaterAwarded.LATER_AWARD_DATE = tmp_qry18c_ExtrLaterAwarded.MaxOfLATER_AWARD_DATE
  AND   tmp_qry18b_ExtrLaterAwarded.concatenated_id = tmp_qry18c_ExtrLaterAwarded.concatenated_id
GROUP BY 
        tmp_qry18b_ExtrLaterAwarded.HID, 
        tmp_qry18b_ExtrLaterAwarded.concatenated_id, 
        tmp_qry18b_ExtrLaterAwarded.LATER_AWARD_DATE, 
        tmp_qry18b_ExtrLaterAwarded.PSI_AWARD_SCHOOL_YEAR;"

# ---- qry19_UpdateDelayDate ----
qry19_UpdateDelayDate <- "
UPDATE  tblCredential_HighestRank 
SET     tblCredential_HighestRank.CREDENTIAL_AWARD_DATE_D_DELAYED = tblCredential_DelayEffect.LATER_AWARD_DATE, 
        tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED = tblCredential_DelayEffect.PSI_AWARD_SCHOOL_YEAR
FROM    tblCredential_HighestRank 
INNER JOIN tblCredential_DelayEffect 
ON tblCredential_HighestRank.ID = tblCredential_DelayEffect.HID;"
