## this is an ad-hoc file, meant only to fix a couple of expr1s for 5 digit NOCs that were incorrectly coded in a previous iteration 

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")


# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")


# model 
tmp_model <- tibble(dbGetQuery(
  decimal_con,
  "SELECT *
  FROM [IDIR\\ALOWERY].tmp_tbl_model
  "
))

tmp_model_fixed <- tmp_model %>% 
  mutate(
    Expr1 = 
    case_when(
      NOC_Level == 5 ~ paste0(
        Age_Group_Rollup_Label, '-', NOC, '-', Current_Region_PSSM_Code_Rollup
      ),
      TRUE ~ Expr1
      )
  )

# check
tmp_model %>% count(Expr1) %>% arrange(desc(n))
tmp_model %>% count(Expr1) %>% count()
tmp_model %>% count()

tmp_model_fixed %>% count(Expr1) %>% arrange(desc(n))
tmp_model_fixed %>% count(Expr1) %>% count()

# save
dbWriteTable(decimal_con,  name = SQL('"IDIR\\LFREDRIC"."tmp_tbl_model"'), tmp_model_fixed)

# QI 
tmp_qi <- tibble(dbGetQuery(
  decimal_con,
  "SELECT *
  FROM [IDIR\\ALOWERY].tmp_tbl_qi
  "
))

tmp_qi_fixed <- tmp_qi %>% 
  mutate(
    Expr1 = 
      case_when(
        NOC_Level == 5 ~ paste0(
          Age_Group_Rollup_Label, '-', NOC, '-', Current_Region_PSSM_Code_Rollup
        ),
        TRUE ~ Expr1
      )
  )

# check
tmp_qi %>% count(Expr1) %>% arrange(desc(n))
tmp_qi %>% count(Expr1) %>% count()
tmp_qi %>% count()

tmp_qi_fixed %>% count(Expr1) %>% arrange(desc(n))
tmp_qi_fixed %>% count(Expr1) %>% count()

# save
dbWriteTable(decimal_con,  name = SQL('"IDIR\\LFREDRIC"."tmp_tbl_qi"'), tmp_qi_fixed)


# tmp_tbl_Model_Inc_Private_Inst
tmp_tbl_Model_Inc_Private_Inst <- tibble(dbGetQuery(
  decimal_con,
  "SELECT *
  FROM [IDIR\\ALOWERY].tmp_tbl_Model_Inc_Private_Inst
  "
))

tmp_tbl_Model_Inc_Private_Inst_fixed <- tmp_tbl_Model_Inc_Private_Inst %>% 
  mutate(
    Expr1 = 
      case_when(
        NOC_Level == 5 ~ paste0(
          Age_Group_Rollup_Label, '-', NOC, '-', Current_Region_PSSM_Code_Rollup
        ),
        TRUE ~ Expr1
      )
  )

# check
tmp_tbl_Model_Inc_Private_Inst %>% count(Expr1) %>% arrange(desc(n))
tmp_tbl_Model_Inc_Private_Inst %>% count(Expr1) %>% count()
tmp_tbl_Model_Inc_Private_Inst %>% count()

tmp_tbl_Model_Inc_Private_Inst_fixed %>% count(Expr1) %>% arrange(desc(n))
tmp_tbl_Model_Inc_Private_Inst_fixed %>% count(Expr1) %>% count()

# save
dbWriteTable(decimal_con,  name = SQL('"IDIR\\LFREDRIC"."tmp_tbl_Model_Inc_Private_Inst"'), tmp_tbl_Model_Inc_Private_Inst_fixed)



## create internal table for Brett
qry <- "
SELECT 
model.Expr1, 
model.Age_Group_Rollup_Label, 
--tmp_tbl_Model.SKILL_LEVEL_CATEGORY_CODE, 
model.NOC_Level, 
--tmp_tbl_Model.NOC_SKILL_TYPE, 
model.NOC, 
model.ENGLISH_NAME, 
model.Current_Region_PSSM_Code_Rollup, 
model.Current_Region_PSSM_Name_Rollup, 
Ceiling([model].[2023/2024]) AS [2023/2024], 
Ceiling([model].[2024/2025]) AS [2024/2025], 
Ceiling([model].[2025/2026]) AS [2025/2026],
Ceiling([model].[2026/2027]) AS [2026/2027], 
Ceiling([model].[2027/2028]) AS [2027/2028], 
Ceiling([model].[2028/2029]) AS [2028/2029],
Ceiling([model].[2029/2030]) AS [2029/2030],
Ceiling([model].[2030/2031]) AS [2030/2031], 
Ceiling([model].[2031/2032]) AS [2031/2032], 
Ceiling([model].[2032/2033]) AS [2032/2033], 
Ceiling([model].[2033/2034]) AS [2033/2034], 
Ceiling([model].[2034/2035]) AS [2034/2035], 
IIf((Abs([model].[2023/2024]-[QI].[2023/2024])/[QI].[2023/2024])<0.25,(Abs([model].[2023/2024]-[QI].[2023/2024])/[QI].[2023/2024]),
IIf(
	[model].[2023/2024]<10 
Or [QI].[2023/2024]<10 
Or [model].[2023/2024]=Null 
Or [QI].[2023/2024]=Null,'999999999',
(Abs([model].[2023/2024]-[QI].[2023/2024])/[QI].[2023/2024]))
) AS [Quality Indicator], 
IIf(
	IsNull([pvt].[2023/2024], 0)=0, 0,
	[model].[2023/2024]/[pvt].[2023/2024]
	) AS [Coverage Indicator]
FROM (tmp_tbl_Model model LEFT JOIN tmp_tbl_QI QI ON model.Expr1 = QI.Expr1) 
LEFT JOIN tmp_tbl_Model_Inc_Private_Inst pvt ON model.Expr1 = pvt.Expr1
WHERE (((model.NOC_Level)=5))
ORDER BY  2, 4, 6
;
"

internal <- dbGetQuery(decimal_con, qry)
internal %>% write_csv(glue::glue("{lan}/development/work/adhoc-outputs/internal-occs-20240926.csv"))
