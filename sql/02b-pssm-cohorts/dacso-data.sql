# Notes: c_outc_clean_short_resp doesn't contain records for subm_cd = C_Outc06 any longer.  Remove from query for next run.
DACSO_Q003_DACSO_DATA_Part_1_stepA <- "
SELECT  surv_cohort_collection_info.coci_pen,
        surv_cohort_collection_info.coci_stqu_id,
        surv_cohort_collection_info.coci_subm_cd,
        surv_cohort_collection_info.coci_lrst_cd,
        surv_cohort_collection_info.coci_inst_cd,
        c_outc_clean_short_resp.pfst_current_activity,
        l_cip_6digits_cip2016.lcip_lcippc_name,
        l_cip_6digits_cip2016.lcip_cd,
        l_cip_4digits_cip2016.lcp4_cd,
        l_cip_4digits_cip2016.lcp4_cip_4digits_name,
        c_outc_clean_short_resp.ttrain,
     	surv_cohort_collection_info.tpid_current_region1,
        surv_cohort_collection_info.tpid_current_region4,
	surv_cohort_collection_info.tpid_lgnd_cd,
        c_outc_clean_short_resp.labr_in_labour_market,
        c_outc_clean_short_resp.q18 AS LABR_EMPLOYED,
        c_outc_clean_short_resp.labr_unemployed,
        c_outc_clean_short_resp.labr_employed_full_part_time,
        c_outc_clean_short_resp.labr_job_search_time_gp,
        c_outc_clean_short_resp.labr_job_training_related,
        c_outc_clean_short_resp.labr_occupation_lnoc_cd,
        surv_cohort_collection_info.coci_age_at_survey,
        0 AS Age_Group,
        0 AS Age_Group_Rollup,
        surv_cohort_collection_info.cosc_grad_status_lgds_cd,
        CASE WHEN cosc_grad_status_lgds_cd = '2' THEN '3' ELSE cosc_grad_status_lgds_cd END AS COSC_GRAD_STATUS_LGDS_CD_Group,
        CASE WHEN coci_lrst_cd = '000' THEN '1' ELSE '' END AS Respondent,
        0 AS New_Labour_Supply,
        0 AS Old_Labour_Supply,
        0 AS Weight,
        0 AS Had_Previous_Credential,
        0 AS PFST_IN_POST_SEC_BEFORE,
        0 AS PFST_HAD_PREVIOUS_CDTL,
        0 AS PFST_FURSTDY_INCL_STILL_ATTD,
        programs.prgm_credential
FROM   c_outc_clean2
RIGHT JOIN ((surv_cohort_collection_info
  INNER JOIN (programs
    INNER JOIN (l_cip_6digits_cip2016
      INNER JOIN l_cip_4digits_cip2016
      ON l_cip_6digits_cip2016.lcip_lcp4_cd  = l_cip_4digits_cip2016.lcp4_cd)
    ON programs.lcip_cd_cip2016 = l_cip_6digits_cip2016.lcip_cd)
  ON surv_cohort_collection_info.cosc_prgm_id = programs.prgm_id)
  LEFT JOIN c_outc_clean_short_resp
    ON surv_cohort_collection_info.coci_stqu_id = c_outc_clean_short_resp.stqu_id)
ON c_outc_clean2.stqu_id = surv_cohort_collection_info.coci_stqu_id
WHERE  (( surv_cohort_collection_info.coci_subm_cd = 'C_Outc23' OR
          surv_cohort_collection_info.coci_subm_cd = 'C_Outc22' OR
          surv_cohort_collection_info.coci_subm_cd = 'C_Outc21' OR
          surv_cohort_collection_info.coci_subm_cd = 'C_Outc20' OR
          surv_cohort_collection_info.coci_subm_cd = 'C_Outc19' OR 
          surv_cohort_collection_info.coci_subm_cd = 'C_Outc18')
  AND     l_cip_6digits_cip2016.lcip_lcippc_name  <> 'Developmental'
  AND     l_cip_6digits_cip2016.lcip_lcippc_name  <> 'Personal Improvement and Leisure')"

infoware_c_outc_clean_short_resp <- "
SELECT *
FROM c_outc_clean_short_resp
WHERE subm_cd IN ('C_Outc18', 'C_Outc19','C_Outc20','C_Outc21','C_Outc22','C_Outc23')
"
