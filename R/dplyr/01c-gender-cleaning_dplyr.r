# ******************************************************************************
# Section 5: Gender cleaning
# WHY: Many records have missing, unknown, or inconsistent gender values across
# enrolment records. This extensive cleaning process resolves multi-gender EPENs
# by selecting the most recent gender, cleans unknowns, and falls back to
# PSI_CODE/PSI_STUDENT_NUMBER matching for null-EPEN records.
#
# The original uses ~35 temp tables. Here we use dplyr pipelines with intermediate
# R variables.
# ******************************************************************************

# ---- 5a: Build CredentialSupVars_Gender — unique EPEN/gender from enrolment ----
csvs_gender <- CredentialSupVarsFromEnrolment %>%
  select(ENCRYPTED_TRUE_PEN, PSI_GENDER) %>%
  distinct()

# ---- 5b: Find EPENs with multiple genders ----
multi_gender_epens <- csvs_gender %>%
  count(ENCRYPTED_TRUE_PEN) %>%
  filter(n > 1) %>%
  select(ENCRYPTED_TRUE_PEN)

# ---- 5c: Resolve multi-gender EPENs ----
# WHY: When an EPEN has multiple genders across enrolment records, we pick the gender
# from the most recent school year, breaking ties by max enrolment sequence.
# Original: Steps 1-5 across 3 temp tables.

if (nrow(multi_gender_epens) > 0) {
  # Max school year + enrolment sequence per EPEN/gender combo
  multi_gender_max <- CredentialSupVarsFromEnrolment %>%
    semi_join(multi_gender_epens, by = "ENCRYPTED_TRUE_PEN") %>%
    group_by(ENCRYPTED_TRUE_PEN, PSI_GENDER) %>%
    summarise(
      MAX_PSI_SCHOOL_YEAR = max(PSI_SCHOOL_YEAR),
      MAX_PSI_ENROLMENT_SEQUENCE = max(PSI_ENROLMENT_SEQUENCE),
      .groups = "drop"
    )

  # Overall max per EPEN
  multi_gender_overall_max <- multi_gender_max %>%
    group_by(ENCRYPTED_TRUE_PEN) %>%
    summarise(
      MAX_MAX_PSI_SCHOOL_YEAR = max(MAX_PSI_SCHOOL_YEAR),
      MAX_MAX_PSI_ENROLMENT_SEQUENCE = max(MAX_PSI_ENROLMENT_SEQUENCE),
      .groups = "drop"
    )

  # Resolve: join to find the gender at the max school year + max enrolment sequence
  multi_gender_resolved <- multi_gender_overall_max %>%
    filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
    inner_join(
      multi_gender_max,
      by = c(
        "ENCRYPTED_TRUE_PEN" = "ENCRYPTED_TRUE_PEN",
        "MAX_MAX_PSI_SCHOOL_YEAR" = "MAX_PSI_SCHOOL_YEAR",
        "MAX_MAX_PSI_ENROLMENT_SEQUENCE" = "MAX_PSI_ENROLMENT_SEQUENCE"
      )
    ) %>%
    select(ENCRYPTED_TRUE_PEN, PSI_GENDER_TO_USE = PSI_GENDER) %>%
    distinct()
} else {
  multi_gender_resolved <- tibble(
    ENCRYPTED_TRUE_PEN = character(),
    PSI_GENDER_TO_USE = character()
  )
}

# ---- 5d: Apply resolved genders to the gender table ----
csvs_gender <- csvs_gender %>%
  left_join(multi_gender_resolved, by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(
    PSI_GENDER_CLEANED_FLAG = if_else(
      !is.na(PSI_GENDER_TO_USE),
      "Yes",
      NA_character_
    ),
    PSI_GENDER_CLEANED = coalesce(PSI_GENDER_TO_USE, PSI_GENDER)
  )

# ---- 5e: Clean unknowns ('U', 'Unknown') by looking at all enrolment genders ----
# WHY: Some EPENs have gender='U' or 'Unknown'. We check if other enrolment records
# for the same EPEN have a valid gender (Male/Female/Gender Diverse).
unknown_genders <- csvs_gender %>%
  filter(PSI_GENDER_CLEANED %in% c("U", "Unknown"))

if (nrow(unknown_genders) > 0) {
  # Find non-U/Unknown genders from enrolment for these EPENs
  unknowns_with_alternatives <- unknown_genders %>%
    select(ENCRYPTED_TRUE_PEN) %>%
    inner_join(
      CredentialSupVarsFromEnrolment %>%
        select(ENCRYPTED_TRUE_PEN, PSI_GENDER) %>%
        distinct(),
      by = "ENCRYPTED_TRUE_PEN"
    ) %>%
    filter(!PSI_GENDER %in% c("U", "Unknown")) %>%
    group_by(ENCRYPTED_TRUE_PEN) %>%
    summarise(GENDERTOUSE = first(PSI_GENDER), .groups = "drop")

  # Update csvs_gender with resolved unknowns
  csvs_gender <- csvs_gender %>%
    left_join(unknowns_with_alternatives, by = "ENCRYPTED_TRUE_PEN") %>%
    mutate(
      PSI_GENDER_CLEANED = if_else(
        PSI_GENDER_CLEANED %in% c("U", "Unknown") & !is.na(GENDERTOUSE),
        GENDERTOUSE,
        PSI_GENDER_CLEANED
      )
    ) %>%
    select(-GENDERTOUSE)
}

# ---- 5f: Apply gender to CredentialSupVars by EPEN ----
# WHY: Update CredentialSupVars PSI_GENDER_CLEANED from the resolved gender table.
CredentialSupVars <- CredentialSupVars %>%
  left_join(
    csvs_gender %>%
      filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
      select(ENCRYPTED_TRUE_PEN, PSI_GENDER_CLEANED),
    by = "ENCRYPTED_TRUE_PEN",
    suffix = c("", "_from_gender")
  ) %>%
  mutate(
    PSI_GENDER_CLEANED = coalesce(
      PSI_GENDER_CLEANED_from_gender,
      PSI_GENDER_CLEANED
    )
  ) %>%
  select(-PSI_GENDER_CLEANED_from_gender)

# ---- 5g: Handle null-EPEN records — match by PSI_CODE/PSI_STUDENT_NUMBER ----
# WHY: Records without a valid EPEN need gender from STP_Enrolment matched by
# PSI_CODE + PSI_STUDENT_NUMBER.
null_gender_records <- CredentialSupVars %>%
  filter(is.na(PSI_GENDER_CLEANED)) %>%
  select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE)

if (nrow(null_gender_records) > 0) {
  # Get genders from STP_Enrolment by PSI_CODE/STUDENT_NUMBER
  enrol_gender_by_stuid <- null_gender_records %>%
    inner_join(
      stp_enrolment %>%
        select(PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER) %>%
        distinct(),
      by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
    )

  # Find EPENs with multiple genders from this match
  multi_gender_stuid <- enrol_gender_by_stuid %>%
    count(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) %>%
    filter(n > 1)

  # Resolve multi-gender by most recent school year (same logic as 5c)
  if (nrow(multi_gender_stuid) > 0) {
    multi_stuid_max <- multi_gender_stuid %>%
      select(-n) %>%
      inner_join(
        CredentialSupVarsFromEnrolment %>%
          select(
            ENCRYPTED_TRUE_PEN,
            PSI_STUDENT_NUMBER,
            PSI_CODE,
            PSI_GENDER,
            PSI_SCHOOL_YEAR,
            PSI_ENROLMENT_SEQUENCE
          ),
        by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE")
      ) %>%
      group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER) %>%
      summarise(
        MAX_PSI_SCHOOL_YEAR = max(PSI_SCHOOL_YEAR),
        MAX_PSI_ENROLMENT_SEQUENCE = max(PSI_ENROLMENT_SEQUENCE),
        .groups = "drop"
      )

    multi_stuid_overall_max <- multi_stuid_max %>%
      group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) %>%
      summarise(
        MAX_MAX_YEAR = max(MAX_PSI_SCHOOL_YEAR),
        MAX_MAX_SEQ = max(MAX_PSI_ENROLMENT_SEQUENCE),
        .groups = "drop"
      )

    multi_stuid_resolved <- multi_stuid_overall_max %>%
      inner_join(
        multi_stuid_max,
        by = c(
          "ENCRYPTED_TRUE_PEN",
          "PSI_STUDENT_NUMBER",
          "PSI_CODE",
          "MAX_MAX_YEAR" = "MAX_PSI_SCHOOL_YEAR",
          "MAX_MAX_SEQ" = "MAX_PSI_ENROLMENT_SEQUENCE"
        )
      ) %>%
      select(
        ENCRYPTED_TRUE_PEN,
        PSI_STUDENT_NUMBER,
        PSI_CODE,
        PSI_GENDER_TO_USE = PSI_GENDER
      ) %>%
      distinct()
  } else {
    multi_stuid_resolved <- tibble(
      ENCRYPTED_TRUE_PEN = character(),
      PSI_STUDENT_NUMBER = character(),
      PSI_CODE = character(),
      PSI_GENDER_TO_USE = character()
    )
  }

  # Build final gender for null-EPEN records
  enrol_gender_final <- enrol_gender_by_stuid %>%
    left_join(
      multi_stuid_resolved,
      by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE")
    ) %>%
    mutate(
      PSI_GENDER_CLEANED_FLAG = if_else(
        !is.na(PSI_GENDER_TO_USE),
        "Yes",
        NA_character_
      )
    ) %>%
    # Clean unknowns from the enrolment match
    mutate(
      PSI_GENDER_CLEANED = coalesce(PSI_GENDER_TO_USE, PSI_GENDER)
    ) %>%
    # If still unknown, keep as-is
    mutate(
      PSI_GENDER_CLEANED_FLAG = if_else(
        PSI_GENDER %in%
          c("U", "Unknown", "(Unspecified)") &
          is.na(PSI_GENDER_CLEANED_FLAG),
        "Yes",
        PSI_GENDER_CLEANED_FLAG
      ),
      PSI_GENDER_CLEANED = if_else(
        is.na(PSI_GENDER_CLEANED_FLAG),
        PSI_GENDER,
        PSI_GENDER_CLEANED
      )
    ) %>%
    select(
      PSI_STUDENT_NUMBER,
      PSI_CODE,
      PSI_GENDER_CLEANED,
      PSI_GENDER_CLEANED_FLAG
    ) %>%
    distinct()

  # Apply to CredentialSupVars
  CredentialSupVars <- CredentialSupVars %>%
    left_join(
      enrol_gender_final %>% filter(PSI_GENDER_CLEANED_FLAG == "Yes"),
      by = c("PSI_STUDENT_NUMBER", "PSI_CODE"),
      suffix = c("", "_enrol")
    ) %>%
    mutate(
      PSI_GENDER_CLEANED = if_else(
        is.na(PSI_GENDER_CLEANED) & !is.na(PSI_GENDER_CLEANED_enrol),
        PSI_GENDER_CLEANED_enrol,
        PSI_GENDER_CLEANED
      )
    ) %>%
    select(-PSI_GENDER_CLEANED_enrol, -PSI_GENDER_CLEANED_FLAG_enrol)
}
