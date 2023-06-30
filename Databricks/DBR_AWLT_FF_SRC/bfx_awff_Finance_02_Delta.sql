-- Databricks notebook source
-- Delta load of bfx_awff_Finance
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
DROP TABLE IF EXISTS `TMP_source_dbo_Finance_CURRENT`;
CREATE TABLE `TMP_source_dbo_Finance_CURRENT` AS 
SELECT	 PSA.`FinanceKey`
		,PSA.
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}``awff`.`Finance` PSA
INNER JOIN
(
    SELECT	 CUR.`FinanceKey`
            ,MAX(CUR.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
    FROM	`${_bfx_ods}`.`awff`.`Finance` CUR
    INNER JOIN `TMP_source_dbo_Finance` TMP
        ON  CUR.`FinanceKey` = TMP.`FinanceKey`
    GROUP BY CUR.`FinanceKey`
) GRP
    ON  PSA.`FinanceKey` = GRP.`FinanceKey`
    AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`;

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
DROP TABLE IF EXISTS `TMP_source_dbo_Finance_EXISTS`;
CREATE TABLE `TMP_source_dbo_Finance_EXISTS` AS 
SELECT	 STG.`FinanceKey`
		,STG.
		,STG.`FlexRowEffectiveFromDate`
FROM	`TMP_source_dbo_Finance_DELTA` STG
INNER JOIN
(
	SELECT	 SRC.`FinanceKey`
			,MIN(SRC.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`TMP_source_dbo_Finance_DELTA` SRC
	GROUP BY SRC.`FinanceKey`
) GRP
	ON	STG.`FinanceKey` = GRP.`FinanceKey`
	AND STG.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
INNER JOIN `TMP_source_dbo_Finance_CURRENT` CUR
    ON	STG.`FinanceKey` = CUR.`FinanceKey`
    AND STG. = CUR.;


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
FROM	`TMP_source_dbo_Finance_DELTA` SRC
LEFT OUTER JOIN `TMP_source_dbo_Finance_CURRENT` TGT
    ON  SRC.`FinanceKey` = TGT.`FinanceKey`
LEFT OUTER JOIN `TMP_source_dbo_Finance_EXISTS` PSA
    ON  SRC.`FinanceKey` = PSA.`FinanceKey`
    AND SRC.`FlexRowEffectiveFromDate` = PSA.`FlexRowEffectiveFromDate`
WHERE   PSA.`FinanceKey` IS NULL;

-- COMMAND ----------
MERGE INTO `${_bfx_ods}`.`awff`.`Finance` AS TGT
USING `${_bfx_stg}`.`awff`.`Finance` AS SRC
    ON  SRC.`FinanceKey` = TGT.`FinanceKey`
WHEN MATCHED 
    AND	NOT (COALESCE(TRIM(SRC.`CustomerID`), 'NVL') = COALESCE(TRIM(TGT.`CustomerID`), 'NVL')
		AND	COALESCE(TRIM(SRC.`Date`), 'NVL') = COALESCE(TRIM(TGT.`Date`), 'NVL')
		AND	COALESCE(TRIM(SRC.`OrganizationAlternateKey`), 'NVL') = COALESCE(TRIM(TGT.`OrganizationAlternateKey`), 'NVL')
		AND	COALESCE(TRIM(SRC.`DepartmentGroupAlternateKey`), 'NVL') = COALESCE(TRIM(TGT.`DepartmentGroupAlternateKey`), 'NVL')
		AND	COALESCE(TRIM(SRC.`ScenarioAlternateKey`), 'NVL') = COALESCE(TRIM(TGT.`ScenarioAlternateKey`), 'NVL')
		AND	COALESCE(TRIM(SRC.`AccountCodeAlternateKey`), 'NVL') = COALESCE(TRIM(TGT.`AccountCodeAlternateKey`), 'NVL')
		AND	COALESCE(TRIM(SRC.`Amount`), 'NVL') = COALESCE(TRIM(TGT.`Amount`), 'NVL'))
    THEN
    UPDATE SET * 
WHEN NOT MATCHED 
    THEN 
    INSERT *;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Finance`;
DROP TABLE IF EXISTS `TMP_source_dbo_Finance_CURRENT`;
DROP TABLE IF EXISTS `TMP_source_dbo_Finance_DELTA`;
DROP TABLE IF EXISTS `TMP_source_dbo_Finance_EXISTS`;

