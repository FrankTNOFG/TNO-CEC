-- Databricks notebook source
-- Full load notebook bfx_awff_Finance
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Finance`;
CREATE TABLE IF NOT EXISTS `TMP_source_dbo_Finance`;

COPY INTO `TMP_source_dbo_Finance`
FROM
(
    SELECT   CAST(`FinanceKey` AS DECIMAL(38,0)) AS `FinanceKey`
			,CAST('awff'||'~'||UPPER(COALESCE(TRIM(STRING(`CustomerID`)),'NVL')) AS STRING) AS `Customer_BK`
			,CAST(`CustomerID` AS DECIMAL(38,0)) AS `CustomerID`
			,CAST(`Date` AS STRING) AS `Date`
			,CAST(`OrganizationAlternateKey` AS INT) AS `OrganizationAlternateKey`
			,CAST(`DepartmentGroupAlternateKey` AS STRING) AS `DepartmentGroupAlternateKey`
			,CAST(`ScenarioAlternateKey` AS STRING) AS `ScenarioAlternateKey`
			,CAST(`AccountCodeAlternateKey` AS STRING) AS `AccountCodeAlternateKey`
			,CAST(`Amount` AS STRING) AS `Amount`
			,CAST('awff'||'~'||UPPER(COALESCE(TRIM(STRING(`AccountCodeAlternateKey`)),'NVL')) AS STRING) AS `Account_BK`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/source_dbo_Finance'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Finance_DELTA`;
CREATE TABLE `TMP_source_dbo_Finance_DELTA` AS
SELECT	 STG.`FinanceKey`
		,STG.`Customer_BK`
		,STG.`CustomerID`
		,STG.`Date`
		,STG.`OrganizationAlternateKey`
		,STG.`DepartmentGroupAlternateKey`
		,STG.`ScenarioAlternateKey`
		,STG.`AccountCodeAlternateKey`
		,STG.`Amount`
		,STG.`Account_BK`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowRecordSource`
		,STG.
FROM
(
	SELECT	 SRC.`FinanceKey`
			,SRC.`Customer_BK`
			,SRC.`CustomerID`
			,SRC.`Date`
			,SRC.`OrganizationAlternateKey`
			,SRC.`DepartmentGroupAlternateKey`
			,SRC.`ScenarioAlternateKey`
			,SRC.`AccountCodeAlternateKey`
			,SRC.`Amount`
			,SRC.`Account_BK`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowRecordSource`
			,SRC.
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`FinanceKey`,SRC. ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`FinanceKey`, SRC. ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.) OVER (PARTITION BY SRC.`FinanceKey` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.) OVER (PARTITION BY SRC.`FinanceKey` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_source_dbo_Finance` AS SRC
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG. <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG. <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awff`.`Finance`;
INSERT INTO `${_bfx_stg}`.`awff`.`Finance`
        (`FinanceKey`
		,`Customer_BK`
		,`CustomerID`
		,`Date`
		,`OrganizationAlternateKey`
		,`DepartmentGroupAlternateKey`
		,`ScenarioAlternateKey`
		,`AccountCodeAlternateKey`
		,`Amount`
		,`Account_BK`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT   SRC.`FinanceKey`
		,SRC.`Customer_BK`
		,SRC.`CustomerID`
		,SRC.`Date`
		,SRC.`OrganizationAlternateKey`
		,SRC.`DepartmentGroupAlternateKey`
		,SRC.`ScenarioAlternateKey`
		,SRC.`AccountCodeAlternateKey`
		,SRC.`Amount`
		,SRC.`Account_BK`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,SRC.`FlexRowRecordSource`
FROM	`TMP_source_dbo_Finance_DELTA` SRC;

-- COMMAND ----------
INSERT INTO `${_bfx_ods}`.`awff`.`Finance`
        (`FinanceKey`
		,`CustomerID`
		,`Date`
		,`OrganizationAlternateKey`
		,`DepartmentGroupAlternateKey`
		,`ScenarioAlternateKey`
		,`AccountCodeAlternateKey`
		,`Amount`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT   INS.`FinanceKey`
		,INS.`CustomerID`
		,INS.`Date`
		,INS.`OrganizationAlternateKey`
		,INS.`DepartmentGroupAlternateKey`
		,INS.`ScenarioAlternateKey`
		,INS.`AccountCodeAlternateKey`
		,INS.`Amount`
		,INS.`FlexRowEffectiveFromDate`
		,INS.`FlexRowAuditId`
		,INS.`FlexRowRecordSource`
FROM	`${_bfx_stg}`.`awff`.`Finance` INS;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Finance`;
DROP TABLE IF EXISTS `TMP_source_dbo_Finance_DELTA`;

