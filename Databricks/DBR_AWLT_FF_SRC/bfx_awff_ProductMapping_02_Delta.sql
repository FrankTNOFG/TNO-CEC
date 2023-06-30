-- Databricks notebook source
-- Delta load of bfx_awff_ProductMapping
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
DROP TABLE IF EXISTS `TMP_source_dbo_ProductMapping_CURRENT`;
CREATE TABLE `TMP_source_dbo_ProductMapping_CURRENT` AS 
SELECT	 PSA.`ProductID`
		,PSA.`ProductCategoryID`
		,PSA.
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}``awff`.`ProductMapping` PSA
INNER JOIN
(
    SELECT	 CUR.`ProductID`
			,CUR.`ProductCategoryID`
            ,MAX(CUR.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
    FROM	`${_bfx_ods}`.`awff`.`ProductMapping` CUR
    INNER JOIN `TMP_source_dbo_ProductMapping` TMP
        ON  CUR.`ProductID` = TMP.`ProductID`
		AND	CUR.`ProductCategoryID` = TMP.`ProductCategoryID`
    GROUP BY CUR.`ProductID`
			,CUR.`ProductCategoryID`
) GRP
    ON  PSA.`ProductID` = GRP.`ProductID`
	AND	PSA.`ProductCategoryID` = GRP.`ProductCategoryID`
    AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`;

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
DROP TABLE IF EXISTS `TMP_source_dbo_ProductMapping_EXISTS`;
CREATE TABLE `TMP_source_dbo_ProductMapping_EXISTS` AS 
SELECT	 STG.`ProductID`
		,STG.`ProductCategoryID`
		,STG.
		,STG.`FlexRowEffectiveFromDate`
FROM	`TMP_source_dbo_ProductMapping_DELTA` STG
INNER JOIN
(
	SELECT	 SRC.`ProductID`
			,SRC.`ProductCategoryID`
			,MIN(SRC.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`TMP_source_dbo_ProductMapping_DELTA` SRC
	GROUP BY SRC.`ProductID`
			,SRC.`ProductCategoryID`
) GRP
	ON	STG.`ProductID` = GRP.`ProductID`
	AND	STG.`ProductCategoryID` = GRP.`ProductCategoryID`
	AND STG.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
INNER JOIN `TMP_source_dbo_ProductMapping_CURRENT` CUR
    ON	STG.`ProductID` = CUR.`ProductID`
	AND	STG.`ProductCategoryID` = CUR.`ProductCategoryID`
    AND STG. = CUR.;


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
FROM	`TMP_source_dbo_ProductMapping_DELTA` SRC
LEFT OUTER JOIN `TMP_source_dbo_ProductMapping_CURRENT` TGT
    ON  SRC.`ProductID` = TGT.`ProductID`
	AND	SRC.`ProductCategoryID` = TGT.`ProductCategoryID`
LEFT OUTER JOIN `TMP_source_dbo_ProductMapping_EXISTS` PSA
    ON  SRC.`ProductID` = PSA.`ProductID`
	AND	SRC.`ProductCategoryID` = PSA.`ProductCategoryID`
    AND SRC.`FlexRowEffectiveFromDate` = PSA.`FlexRowEffectiveFromDate`
WHERE   PSA.`ProductID` IS NULL;

-- COMMAND ----------
MERGE INTO `${_bfx_ods}`.`awff`.`ProductMapping` AS TGT
USING `${_bfx_stg}`.`awff`.`ProductMapping` AS SRC
    ON  SRC.`ProductID` = TGT.`ProductID`
	AND	SRC.`ProductCategoryID` = TGT.`ProductCategoryID`
WHEN MATCHED 
    AND	NOT ()
    THEN
    UPDATE SET * 
WHEN NOT MATCHED 
    THEN 
    INSERT *;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_ProductMapping`;
DROP TABLE IF EXISTS `TMP_source_dbo_ProductMapping_CURRENT`;
DROP TABLE IF EXISTS `TMP_source_dbo_ProductMapping_DELTA`;
DROP TABLE IF EXISTS `TMP_source_dbo_ProductMapping_EXISTS`;

