WITH base_data AS (
  SELECT
    DE_transactions.caseId AS RECORD_SOURCE,
    SUBSTR(TRIM(LEADING 'cfg-obi-' FROM DE_transactions.caseId), 1, 
           INSTR(TRIM(LEADING 'cfg-obi-' FROM DE_transactions.caseId), '-') - 1) AS DOSSIER,
    SUBSTR(iban, 5, 8) AS BANKCODE,
    ROUND(amount, 2) AS AMOUNT,
    TO_DATE(valueDate, 'YYYY-MM-DD', 1) AS VALUTA,
    CASE 
      WHEN sens_cat.BOBCAT_ID IS NOT NULL THEN 'anonymized'
      WHEN amount <= 0 THEN DE_transactions.counterpartiesPayeeName
      WHEN amount > 0 THEN DE_transactions.counterpartiesPayerName
      ELSE NULL
    END AS PARTNERNAME,
    NULL AS PARTNERBANKIDENTIFIERCODE,
    NULL AS FACTSID,
    NULL AS FACTSCATEGORY,
    CASE 
      WHEN sens_cat.BOBCAT_ID IS NOT NULL THEN 'anonymized'
      ELSE CONCAT(DE_transactions.aggregatorDescription, DE_transactions.aggregatorDetailedDescription)
    END AS ORIGINALPURPOSE,
    DE_accounts.IBAN AS IBAN,
    TO_DATE(operationDate, 'YYYY-MM-DD', 1) AS BOOKINGDATE,
    BIC,
    CASE 
      WHEN amount <= 0 THEN DE_transactions.counterpartiesPayeeAccountNumber
      WHEN amount > 0 THEN DE_transactions.counterpartiesPayerAccountNumber
      ELSE NULL
    END AS PARTNERACCOUNTIDENTIFIER,
    'PW' AS DATASOURCE,
    CASE 
      WHEN sens_cat.BOBCAT_ID IS NOT NULL THEN 'anonymized'
      ELSE DE_transactions.categoryLabel
    END AS BOBCAT_CATEGORY,
    NULL AS BALANCE,
    NULL AS RECURRENCE_ID,
    NULL AS RECURRENCE_FLAG,
    ROW_NUMBER() OVER (
      PARTITION BY amount, iban, bookingdate, partneraccountidentifier, originalpurpose
      ORDER BY valuta DESC
    ) AS rn
  FROM "BNPP_PF_PRD"."BU_POP_OBA".Germany."Banking_Open_Data"."DE_transactions"
  JOIN "BNPP_PF_PRD"."BU_POP_OBA".Germany."Banking_Open_Data"."DE_accounts"
    ON DE_transactions.caseId = DE_accounts.caseId
  LEFT JOIN "GERMANY_DWH"."INFORMATION_MART"."GE_R_OBI_SENSITIVE_CATEGORIES" sens_cat
    ON DE_transactions.categoryId = sens_cat.BOBCAT_ID AND sens_cat.OUT_CRITICAL_FLG = 1
  JOIN "Germany_CDO".DATA."IM_REVENUES_AUTHORIZATION"
    ON facility_business_id = SUBSTR(TRIM(LEADING 'cfg-obi-' FROM DE_transactions.caseId), 1, 
                                     INSTR(TRIM(LEADING 'cfg-obi-' FROM DE_transactions.caseId), '-') - 1)
)

SELECT * FROM base_data

UNION ALL

SELECT
  RECORD_SOURCE,
  DOSSIER,
  BANKCODE,
  AMOUNT,
  TO_DATE(VALUTA, 'YYYY-MM-DD', 1),
  PARTNERNAME,
  PARTNERBANKIDENTIFIERCODE,
  FACTSID,
  FACTSCATEGORY,
  ORIGINALPURPOSE,
  IBAN,
  TO_DATE(BOOKINGDATE, 'YYYY-MM-DD', 1),
  BIC,
  PARTNERACCOUNTIDENTIFIER,
  DATASOURCE,
  NULL AS BOBCAT_CATEGORY,
  NULL AS BALANCE,
  NULL AS RECURRENCE_ID,
  NULL AS RECURRENCE_FLAG,
  rn
FROM "Germany_CDO".DATA."IM_DC_REVENUES";
