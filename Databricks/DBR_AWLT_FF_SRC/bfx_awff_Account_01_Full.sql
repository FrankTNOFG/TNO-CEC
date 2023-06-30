-- Databricks notebook source
-- Full load notebook bfx_awff_Account
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
FROM	`TMP_source_dbo_Account_DELTA` SRC;

-- COMMAND ----------
INSERT INTO `${_bfx_ods}`.`awff`.`Account`
        (`AccountCodeAlternateKey`
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
SELECT   INS.`AccountCodeAlternateKey`
		,INS.`ParentAccountCodeAlternateKey`
		,INS.`AccountDescription`
		,INS.`AccountType`
		,INS.`Operator`
		,INS.`CustomMembers`
		,INS.`ValueType`
		,INS.`CustomMemberOptions`
		,INS.`FlexRowEffectiveFromDate`
		,INS.`FlexRowAuditId`
		,INS.`FlexRowRecordSource`
FROM	`${_bfx_stg}`.`awff`.`Account` INS;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Account`;
DROP TABLE IF EXISTS `TMP_source_dbo_Account_DELTA`;

