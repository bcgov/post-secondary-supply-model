---
description: 'Rmarkdown language and document formats (Quarto): coding standards and Copilot guidance for idiomatic, safe, and consistent code generation.'
applyTo: '**/*technical-documentation*.md, **/*technical-documentation*.qmd'
---

# Markdown / Quarto Language Instructions

## Purpose

Help GitHub Copilot generate idiomatic, safe, and maintainable technical documentation within this project.

## Core Conventions

- **Match the template’s style.** If the methodology shows a preference in syntax, grammar or voice (i.e. dispassionate, formal, casual), follow it.
- **Prefer clear sections aligned with individual methodologies.** Keep methodology sub-sections specific to a one idea or transformation method.
- **Naming:** When referring to code, use `UPPER_SNAKE_CASE` for constants and `lower_snake_case` for table names; avoid dots in names.
- **Safety:** Never release personal information or information that could identify individuals.  Never include secrets.

## Mathmematical Conventions

- **Random Variables:** Use upper case letters (e.g., `N`, `X`) for random variables.  Use lower case letters (e.g., `i`, `j`) for subscripts.
- **Equations:** Use multi-line equations when the expression is too large to fit in the standard inter-line spacing of the paragraph.  Use the `align*` environment when writing multi-line equations.

## Security Best Practices

- **File paths:** Never display the full path name.  Hide internal directory structures by ommitting LAN drive letters (e.g. `P:\\`) and project numbers (e.g. `25-2465`)
- **Credentials:** Never hardcode secrets.  Refer to a config file, if one exists.

## R Markdown / Quarto

- Keep chunks focused; prefer explicit chunk options (`echo`, `message`, `warning`).
- Avoid global state; prefer local helpers. Use `withr::with_seed()` for deterministic chunks.

## Data Dictionary

- **Create a data dictionary:** Create a data dictionary for each key output and input table.  
- **Required Information:** Define the columns, data types, and descriptions for each table.
- **Structure:** Use a html list to style elements (e.g. <ul class="list-variable-names"><li>Column Name (Datatype): Description</li></ul>) 

## Copilot-Specific Guidance

- If R code chunks in the current file use tidyverse, **suggest tidyverse-first patterns** (e.g., `dplyr::across()` instead of superseded verbs). If base-R style is present, **use base idioms**.
- Suggest vectorized or tidy solutions over loops when idiomatic.
- Prefer small helper functions over long pipelines.
- When multiple approaches are equivalent, prefer readability and type stability and explain the trade-offs.

---

## Minimal Examples

The `population_projections` dataset is transformed into a long-format structure to facilitate province-wide analysis. Sub-provincial geographic identifiers are removed and age-specific columns are combined into a singular `AGE_GROUP` variable. The cohort is restricted to binary gender categories.

##### Graduate Forecasting
A 2-year annual stratified graduation rate, $R_G$, is calculated from graduate and enrollment counts for 2021 and 2022 acedemic years.  $R_G = \frac{N_G}{N_E}$  This rate is subsequently applied back to the forecasted enrollment counts generate anticipated graduates $N_G = R_G \times N_F$.

##### Inclusion of Near-Completers
The "Near-Completer" population is estimated by applying credential-specific ratios, $R_{C_{NC}}$ calculated in a previous analysis (see `03-near-completers-ttrain.R`).  Near completer ratios ire applied to the forecasted grad count to derive an estimate of near completer graduates, $N_G_{NC}$ = $N_G \times R_{C_{NC}}$, who have completed significant program requirements without formal credential attainment. 

##### Preprocessing
APPSO records from the STP credential (credential_non_dup) table are isolated for processing.  Light pre-processing was done on the STP Credential CIP column, PSI_CREDENTIAL_CIP.  Before matching, the PSI_CREDENTIAL_CIP strings are cleaned to correct common formatting inconsistencies:

- Length-6 Padding: If a CIP is 6 characters and the second character is not a period (e.g., 123456), a trailing 0 is appended.
- Leading Zeros: If a CIP is still only 6 characters, a leading 0 is prepended to ensure a standardized length.


##### Credential_Non_Dup_STP_APPSO_Cleaning

A summary table containing unique CIP codes and their standardized mappings.

<ul class="list-variable-names">
  <li>PSI_CREDENTIAL_CIP (String): The normalized and padded version of the CIP code used as the primary joining key for standardization.</li>
  <li>OUTCOMES_CRED (String): Identifies the survey category (typically "APPSO" for the apprenticeship cohort).</li>
  <li>Expr1 (Number): A calculated field representing the count of occurrences for each unique CIP string within the source data.</li>
  <li>STP_CIP_CODE_4 (String): The standardized 4-digit CIP code successfully mapped from the provincial lookup tables.</li>
  <li>STP_CIP_CODE_4_NAME (String): The official provincial title associated with the 4-digit CIP code (e.g., "Carpentry/Carpenter").</li>
  <li>STP_CIP_CODE_2 (String): The standardized 2-digit CIP "family" code derived from the 4-digit match.</li>
  <li>STP_CIP_CODE_2_NAME (String): The official provincial title for the 2-digit CIP category (e.g., "Construction Trades").</li>
  <li>PSI_CREDENTIAL_CIP_orig (String): The original, raw CIP string as it appeared in the institutional submission before normalization.</li>
</ul>


```r
# Base R variant
scores <- data.frame(id = 1:5, x = c(1, 3, 2, 5, 4))
safe_log <- function(x) tryCatch(log(x), error = function(e) NA_real_)
scores$z <- vapply(scores$x, safe_log, numeric(1))

# Tidyverse variant (if this file uses tidyverse)
result <- tibble::tibble(id = 1:5, x = c(1, 3, 2, 5, 4)) |>
dplyr::mutate(z = purrr::map_dbl(x, purrr::possibly(log, otherwise = NA_real_))) |>
dplyr::filter(z > 0)

# Example reusable helper with roxygen2 doc
#' Compute the z-score of a numeric vector
#' @param x A numeric vector
#' @return Numeric vector of z-scores
#' @examples z_score(c(1, 2, 3))
z_score <- function(x) (x - mean(x, na.rm = TRUE)) / stats::sd(x, na.rm = TRUE)
```
