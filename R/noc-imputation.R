
# ---- Target summary table ----
# Note that there is suppression even here compared to total Age, Field of Study
all_occupations_summary <- data %>%
  filter(occupation_NOC=="All occupations") %>%
  group_by(age_group,major_field_cip) %>%
  summarize(All_Occs_Above_Bach=sum(!!rlang::sym(above_bach_var)),
            All_Occs_PDEG=sum(!!rlang::sym(pdeg_var)),
            All_Occs_Combined=sum(!!rlang::sym(combined_masters_doctorate_var)),
            All_Occs_Masters=sum(!!rlang::sym(masters_var)),
            All_Occs_Doctorate=sum(!!rlang::sym(doctorate_var)))

# Parse NOC codes ----
# NOC 2021 codes are now 5-digits (compared to 4-digits for prior NOC 2016)
# stat_can_data %>% filter(!str_detect(occupation_NOC,"^\\d{1,5} ")) %>% pull(occupation_NOC) %>% unique()
# [1] "Total - Occupation - Unit group - National Occupational Classification (NOC) 2021"
# [2] "Occupation - not applicable"
# [3] "All occupations"

data_working <- data %>%
  mutate(NOC_1 = ifelse(str_detect(occupation_NOC,"^\\d{1,5} "),str_sub(occupation_NOC,1,1),NA),
         NOC_2 = ifelse(str_detect(occupation_NOC,"^\\d{2,5} "),str_sub(occupation_NOC,1,2),NA),
         NOC_3 = ifelse(str_detect(occupation_NOC,"^\\d{3,5} "),str_sub(occupation_NOC,1,3),NA),
         NOC_4 = ifelse(str_detect(occupation_NOC,"^\\d{4,5} "),str_sub(occupation_NOC,1,4),NA),
         NOC_5 = ifelse(str_detect(occupation_NOC,"^\\d{5} "),str_sub(occupation_NOC,1,5),NA)) %>%
  filter(!(is.na(NOC_1) & is.na(NOC_2) & is.na(NOC_3) & is.na(NOC_4) & is.na(NOC_5)))

# Summary table by 4D NOC ----
NOC_4_summary <- data_working %>%
  filter(!is.na(NOC_4) & is.na(NOC_5)) %>%
  group_by(age_group,major_field_cip) %>%
  summarize(NOC4_Above_Bach=sum(!!rlang::sym(above_bach_var)),
            NOC4_PDEG=sum(!!rlang::sym(pdeg_var)),
            NOC4_Combined=sum(!!rlang::sym(combined_masters_doctorate_var)),
            NOC4_Masters=sum(!!rlang::sym(masters_var)),
            NOC4_Doctorate=sum(!!rlang::sym(doctorate_var)))

# Summary table by 5D NOC ----
NOC_5_summary_1 <- data_working %>%
  filter(!is.na(NOC_5)) %>%
  group_by(age_group,major_field_cip) %>%
  summarize(NOC5_Above_Bach=sum(!!rlang::sym(above_bach_var)),
            NOC5_PDEG=sum(!!rlang::sym(pdeg_var)),
            NOC5_Combined=sum(!!rlang::sym(combined_masters_doctorate_var)),
            NOC5_Masters=sum(!!rlang::sym(masters_var)),
            NOC5_Doctorate=sum(!!rlang::sym(doctorate_var)))


# Impute NOC 5 counts ----

# Append 4D NOC code totals as a new row (i.e., repeated for every 5D sub code)
# Reduce to only 5D rows
# For each 4D code, impute values where 5D code is 0

NOC_4 <- data_working %>%
  filter(!is.na(NOC_4) & is.na(NOC_5)) %>%
  select(-NOC_1,-NOC_2,-NOC_3,-NOC_5) %>%
  rename(NOC4_Total:=!!total_var,
         NOC4_Above_Bach:=!!above_bach_var,
         NOC4_PDEG:=!!pdeg_var,
         NOC4_Combined:=!!combined_masters_doctorate_var,
         NOC4_Masters:=!!masters_var,
         NOC4_Doctorate:=!!doctorate_var) %>%
  select(age_group,major_field_cip,NOC_4,NOC4_Total,NOC4_Above_Bach,NOC4_PDEG,NOC4_Masters,NOC4_Doctorate,NOC4_Combined)

data_fill_by_noc <- data_working %>%
  filter(!is.na(NOC_5)) %>%
  select(age_group,major_field_cip,NOC_4,NOC_5, occupation_NOC,
         Total:=!!total_var, !!rlang::sym(above_bach_var),!!rlang::sym(pdeg_var),!!rlang::sym(masters_var),!!rlang::sym(doctorate_var),Combined:=!!combined_masters_doctorate_var) %>%
  left_join(NOC_4, by=c("age_group","major_field_cip","NOC_4"))

data_fill_by_noc_list <- data_fill_by_noc %>%
  group_by(age_group,major_field_cip,NOC_4) %>%
  nest()

## Quick look at number of 4D NOC codes, where 5D sum to more/less than 4D NOC totals
# For Master's degree
# data_fill_by_noc %>%
#   group_by(age_group,major_field_cip,NOC_4,NOC4_Masters) %>%
#   filter(NOC4_Masters!=0) %>%
#   summarize(sum_m=sum(!!rlang::sym(masters_var))) %>%
#   mutate(Under=ifelse(sum_m < NOC4_Masters, 1,0), Over=ifelse(sum_m > NOC4_Masters, 1,0),Even=ifelse(sum_m == NOC4_Masters, 1,0)) %>%
#   ungroup() %>%
#   summarize(Count_under=sum(Under),Count_over=sum(Over),Count_even=sum(Even))

# Function for filling in 5D NOCs
fill_missing_by_NOC <- function(data,col_to_fill,col_total,filler_col,filler_total) {
  # Create new overall total column as the Total NOC4 count - the NOC5 counts where the credential count is not missing
  ## Sometimes this can result in a negative number due to Census rounding -> To handle this, take the max between the new overall total and 0
  # similarly, create new credential total
  # Then fill in the missing credential counts as the New credential total * total count / new overall total
  ## Some exceptions:
  ### If New_Total_Total==0; new value = 0 to avoid division by 0
  ### Total is bigger than New_Total_Total (due to census rounding) new value= new credential total (i.e., divide by 1)

  get_Sum_Total <- function(df) {
    sum(data = df$Total)
  }


  data %>%
    rename(Credential=!!col_to_fill,Cred_Total=!!col_total,Total=!!filler_col,Total_Total=!!filler_total) %>%
    nest() %>%
    mutate(Sum_Total=map_dbl(data,get_Sum_Total)) %>%
    unnest_legacy() %>%
    mutate(New_Total_Total=max(Sum_Total-sum(ifelse(Credential==0,0,Total)),0),
           New_Cred_Total=max(Cred_Total-sum(Credential),0),
           New_Credential=ifelse(Credential==0,
                                 ifelse(New_Total_Total == 0,
                                        0,
                                        ifelse(Total > New_Total_Total,
                                               New_Cred_Total,
                                               New_Cred_Total*Total/New_Total_Total)),
                                 Credential))
}

# ** University certificate or diploma above bachelor level ----
above_bach <- map_dfr(seq(1:nrow(data_fill_by_noc_list)), ~ fill_missing_by_NOC(unnest_legacy(data_fill_by_noc_list[.x,]),
                                                                                col_to_fill=above_bach_var,
                                                                                col_total="NOC4_Above_Bach",
                                                                                filler_col="Total",
                                                                                filler_total="NOC4_Total")) %>%
  select(age_group,major_field_cip, NOC_4, NOC_5, occupation_NOC, New_Above_Bach=New_Credential)

# ** Degree in medicine, dentistry, veterinary medicine or optometry ----
pdeg <- map_dfr(seq(1:nrow(data_fill_by_noc_list)), ~ fill_missing_by_NOC(unnest_legacy(data_fill_by_noc_list[.x,]),
                                                                          col_to_fill=pdeg_var,
                                                                          col_total="NOC4_PDEG",
                                                                          filler_col="Total",
                                                                          filler_total="NOC4_Total")) %>%
  select(age_group,major_field_cip, NOC_4, NOC_5, occupation_NOC, New_PDEG=New_Credential)

# ** Master's ----
masters <- map_dfr(seq(1:nrow(data_fill_by_noc_list)), ~ fill_missing_by_NOC(unnest_legacy(data_fill_by_noc_list[.x,]),
                                                                             col_to_fill=masters_var,
                                                                             col_total="NOC4_Masters",
                                                                             filler_col="Total",
                                                                             filler_total="NOC4_Total")) %>%
  select(age_group,major_field_cip, NOC_4, NOC_5, occupation_NOC, New_Masters=New_Credential)

# ** Doctorate ----
doctorate <- map_dfr(seq(1:nrow(data_fill_by_noc_list)), ~ fill_missing_by_NOC(unnest_legacy(data_fill_by_noc_list[.x,]),
                                                                               col_to_fill=doctorate_var,
                                                                               col_total="NOC4_Doctorate",
                                                                               filler_col="Total",
                                                                               filler_total="NOC4_Total")) %>%
  select(age_group,major_field_cip, NOC_4, NOC_5, occupation_NOC, New_Doctorate=New_Credential)

# ** Combined ----
combined <- map_dfr(seq(1:nrow(data_fill_by_noc_list)), ~ fill_missing_by_NOC(unnest_legacy(data_fill_by_noc_list[.x,]),
                                                                              col_to_fill="Combined",
                                                                              col_total="NOC4_Combined",
                                                                              filler_col="Total",
                                                                              filler_total="NOC4_Total")) %>%
  select(age_group,major_field_cip, NOC_4, NOC_5, occupation_NOC, New_Combined=New_Credential)

# ** Join new counts ----
new_noc_counts <- left_join(above_bach,pdeg,by=c("age_group", "major_field_cip", "NOC_4", "NOC_5", "occupation_NOC")) %>%
  left_join(combined,by=c("age_group", "major_field_cip", "NOC_4", "NOC_5", "occupation_NOC")) %>%
  left_join(masters,by=c("age_group", "major_field_cip", "NOC_4", "NOC_5", "occupation_NOC")) %>%
  left_join(doctorate,by=c("age_group", "major_field_cip", "NOC_4", "NOC_5", "occupation_NOC"))


print("num of NEW NOC4 Masters != 0")
new_noc_counts %>% filter(New_Masters!=0) %>% tally() %>% pull(n) %>% print()

# New Summary table by 5D NOC ----
NOC_5_summary_2 <- new_noc_counts %>%
  group_by(age_group,major_field_cip) %>%
  summarize(NEW_NOC4_Above_Bach=sum(New_Above_Bach),
            NEW_NOC4_PDEG=sum(New_PDEG),
            NEW_NOC4_Combined=sum(New_Combined),
            NEW_NOC4_Masters=sum(New_Masters),
            NEW_NOC4_Doctorate=sum(New_Doctorate))

# Combine summary tables ----
compare_summaries <- all_occupations_summary %>%
  left_join(NOC_4_summary,by=c("age_group","major_field_cip")) %>%
  left_join(NOC_5_summary_1,by=c("age_group","major_field_cip")) %>%
  left_join(NOC_5_summary_2,by=c("age_group","major_field_cip"))

# add total row
compare_summaries <- compare_summaries %>%
  bind_rows(compare_summaries %>%
              ungroup() %>%
              select(-age_group,-major_field_cip) %>%
              summarize_all(sum) %>%
              mutate(age_group="Total", major_field_cip="Total"))

# Save files ----
write_csv(new_noc_counts,newcounts_fn)
write_csv(compare_summaries,summary_fn)

