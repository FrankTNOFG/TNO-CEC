-- Databricks notebook source
-- Full load notebook bfx_awff_ProductMapping
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_ProductMapping`;
CREATE TABLE IF NOT EXISTS `TMP_source_dbo_ProductMapping`;

COPY INTO `TMP_source_dbo_ProductMapping`
FROM
(
    SELECT   CAST('awff'||'~'||UPPER(COALESCE(TRIM(STRING(`ProductID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(`ProductCategoryID`)),'NVL')) AS STRING) AS `ProductMapping_BK`
			,CAST(`ProductID` AS INT) AS `ProductID`
			,CAST(`ProductCategoryID` AS INT) AS `ProductCategoryID`
			,CAST('awff'||'~'||UPPER(COALESCE(TRIM(STRING(`ProductID`)),'NVL')) AS STRING) AS `Product_BK`
			,CAST('awff'||'~'||UPPER(COALESCE(TRIM(STRING(`ProductCategoryID`)),'NVL')) AS STRING) AS `ProductCategory_BK`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/source_dbo_ProductMapping'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_ProductMapping_DELTA`;
CREATE TABLE `TMP_source_dbo_ProductMapping_DELTA` AS
SELECT	 STG.`ProductMapping_BK`
		,STG.`ProductID`
		,STG.`ProductCategoryID`
		,STG.`Product_BK`
		,STG.`ProductCategory_BK`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowRecordSource`
		,STG.
FROM
(
	SELECT	 SRC.`ProductMapping_BK`
			,SRC.`ProductID`
			,SRC.`ProductCategoryID`
			,SRC.`Product_BK`
			,SRC.`ProductCategory_BK`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowRecordSource`
			,SRC.
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`ProductID`,SRC.`ProductCategoryID`,SRC. ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`ProductID`,SRC.`ProductCategoryID`, SRC. ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.) OVER (PARTITION BY SRC.`ProductID`,SRC.`ProductCategoryID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.) OVER (PARTITION BY SRC.`ProductID`,SRC.`ProductCategoryID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_source_dbo_ProductMapping` AS SRC
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG. <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG. <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awff`.`ProductMapping`;
INSERT INTO `${_bfx_stg}`.`awff`.`ProductMapping`
        (`ProductMapping_BK`
		,`ProductID`
		,`ProductCategoryID`
		,`Product_BK`
		,`ProductCategory_BK`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT   SRC.`ProductMapping_BK`
		,SRC.`ProductID`
		,SRC.`ProductCategoryID`
		,SRC.`Product_BK`
		,SRC.`ProductCategory_BK`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,SRC.`FlexRowRecordSource`
FROM	`TMP_source_dbo_ProductMapping_DELTA` SRC;

-- COMMAND ----------
INSERT INTO `${_bfx_ods}`.`awff`.`ProductMapping`
        (`ProductID`
		,`ProductCategoryID`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT   INS.`ProductID`
		,INS.`ProductCategoryID`
		,INS.`FlexRowEffectiveFromDate`
		,INS.`FlexRowAuditId`
		,INS.`FlexRowRecordSource`
FROM	`${_bfx_stg}`.`awff`.`ProductMapping` INS;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_ProductMapping`;
DROP TABLE IF EXISTS `TMP_source_dbo_ProductMapping_DELTA`;

