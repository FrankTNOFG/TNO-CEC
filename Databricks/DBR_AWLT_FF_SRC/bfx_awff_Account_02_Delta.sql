-- Databricks notebook source
-- Delta load of bfx_awff_Account
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Account`;
CREATE TABLE IF NOT EXISTS `TMP_source_dbo_Account`;

COPY INTO `TMP_source_dbo_Account`
FROM
(
    SELECT   CAST('awff'||'~'||UPPER(COALESCE(TRIM(STRING(`AccountCodeAlternateKey`)),'NVL')) AS STRING) AS `Account_BK`
			,CAST(`AccountCodeAlternateKey` AS INT) AS `AccountCodeAlternateKey`
			,CAST(`ParentAccountCodeAlternateKey` AS INT) AS `ParentAccountCodeAlternateKey`
			,CAST(`AccountDescription` AS STRING) AS `AccountDescription`
			,CAST(`AccountType` AS STRING) AS `AccountType`
			,CAST(`Operator` AS STRING) AS `Operator`
			,CAST(`CustomMembers` AS STRING) AS `CustomMembers`
			,CAST(`ValueType` AS STRING) AS `ValueType`
			,CAST(`CustomMemberOptions` AS STRING) AS `CustomMemberOptions`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/source_dbo_Account'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Account_CURRENT`;
CREATE TABLE `TMP_source_dbo_Account_CURRENT` AS 
SELECT	 PSA.`AccountCodeAlternateKey`
		,PSA.
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}``awff`.`Account` PSA
INNER JOIN
(
    SELECT	 CUR.`AccountCodeAlternateKey`
            ,MAX(CUR.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
    FROM	`${_bfx_ods}`.`awff`.`Account` CUR
    INNER JOIN `TMP_source_dbo_Account` TMP
        ON  CUR.`AccountCodeAlternateKey` = TMP.`AccountCodeAlternateKey`
    GROUP BY CUR.`AccountCodeAlternateKey`
) GRP
    ON  PSA.`AccountCodeAlternateKey` = GRP.`AccountCodeAlternateKey`
    AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Account_DELTA`;
CREATE TABLE `TMP_source_dbo_Account_DELTA` AS
SELECT	 STG.`Account_BK`
		,STG.`AccountCodeAlternateKey`
		,STG.`ParentAccountCodeAlternateKey`
		,STG.`AccountDescription`
		,STG.`AccountType`
		,STG.`Operator`
		,STG.`CustomMembers`
		,STG.`ValueType`
		,STG.`CustomMemberOptions`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowRecordSource`
		,STG.
FROM
(
	SELECT	 SRC.`Account_BK`
			,SRC.`AccountCodeAlternateKey`
			,SRC.`ParentAccountCodeAlternateKey`
			,SRC.`AccountDescription`
			,SRC.`AccountType`
			,SRC.`Operator`
			,SRC.`CustomMembers`
			,SRC.`ValueType`
			,SRC.`CustomMemberOptions`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowRecordSource`
			,SRC.
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`AccountCodeAlternateKey`,SRC. ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`AccountCodeAlternateKey`, SRC. ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.) OVER (PARTITION BY SRC.`AccountCodeAlternateKey` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.) OVER (PARTITION BY SRC.`AccountCodeAlternateKey` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_source_dbo_Account` AS SRC
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG. <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG. <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Account_EXISTS`;
CREATE TABLE `TMP_source_dbo_Account_EXISTS` AS 
SELECT	 STG.`AccountCodeAlternateKey`
		,STG.
		,STG.`FlexRowEffectiveFromDate`
FROM	`TMP_source_dbo_Account_DELTA` STG
INNER JOIN
(
	SELECT	 SRC.`AccountCodeAlternateKey`
			,MIN(SRC.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`TMP_source_dbo_Account_DELTA` SRC
	GROUP BY SRC.`AccountCodeAlternateKey`
) GRP
	ON	STG.`AccountCodeAlternateKey` = GRP.`AccountCodeAlternateKey`
	AND STG.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
INNER JOIN `TMP_source_dbo_Account_CURRENT` CUR
    ON	STG.`AccountCodeAlternateKey` = CUR.`AccountCodeAlternateKey`
    AND STG. = CUR.;


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awff`.`Account`;
INSERT INTO `${_bfx_stg}`.`awff`.`Account`
        (`Account_BK`
		,`AccountCodeAlternateKey`
		,`ParentAccountCodeAlternateKey`
		,`AccountDescription`
		,`AccountType`
		,`Operator`
		,`CustomMembers`
		,`ValueType`
		,`CustomMemberOptions`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT   SRC.`Account_BK`
		,SRC.`AccountCodeAlternateKey`
		,SRC.`ParentAccountCodeAlternateKey`
		,SRC.`AccountDescription`
		,SRC.`AccountType`
		,SRC.`Operator`
		,SRC.`CustomMembers`
		,SRC.`ValueType`
		,SRC.`CustomMemberOptions`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,SRC.`FlexRowRecordSource`
FROM	`TMP_source_dbo_Account_DELTA` SRC
LEFT OUTER JOIN `TMP_source_dbo_Account_CURRENT` TGT
    ON  SRC.`AccountCodeAlternateKey` = TGT.`AccountCodeAlternateKey`
LEFT OUTER JOIN `TMP_source_dbo_Account_EXISTS` PSA
    ON  SRC.`AccountCodeAlternateKey` = PSA.`AccountCodeAlternateKey`
    AND SRC.`FlexRowEffectiveFromDate` = PSA.`FlexRowEffectiveFromDate`
WHERE   PSA.`AccountCodeAlternateKey` IS NULL;

-- COMMAND ----------
MERGE INTO `${_bfx_ods}`.`awff`.`Account` AS TGT
USING `${_bfx_stg}`.`awff`.`Account` AS SRC
    ON  SRC.`AccountCodeAlternateKey` = TGT.`AccountCodeAlternateKey`
WHEN MATCHED 
    AND	NOT (COALESCE(TRIM(CAST(SRC.`ParentAccountCodeAlternateKey` AS STRING)), 'NVL') = COALESCE(TRIM(CAST(TGT.`ParentAccountCodeAlternateKey` AS STRING)), 'NVL')
		AND	COALESCE(TRIM(SRC.`AccountDescription`), 'NVL') = COALESCE(TRIM(TGT.`AccountDescription`), 'NVL')
		AND	COALESCE(TRIM(SRC.`AccountType`), 'NVL') = COALESCE(TRIM(TGT.`AccountType`), 'NVL')
		AND	COALESCE(TRIM(SRC.`Operator`), 'NVL') = COALESCE(TRIM(TGT.`Operator`), 'NVL')
		AND	COALESCE(TRIM(SRC.`CustomMembers`), 'NVL') = COALESCE(TRIM(TGT.`CustomMembers`), 'NVL')
		AND	COALESCE(TRIM(SRC.`ValueType`), 'NVL') = COALESCE(TRIM(TGT.`ValueType`), 'NVL')
		AND	COALESCE(TRIM(SRC.`CustomMemberOptions`), 'NVL') = COALESCE(TRIM(TGT.`CustomMemberOptions`), 'NVL'))
    THEN
    UPDATE SET * 
WHEN NOT MATCHED 
    THEN 
    INSERT *;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Account`;
DROP TABLE IF EXISTS `TMP_source_dbo_Account_CURRENT`;
DROP TABLE IF EXISTS `TMP_source_dbo_Account_DELTA`;
DROP TABLE IF EXISTS `TMP_source_dbo_Account_EXISTS`;

