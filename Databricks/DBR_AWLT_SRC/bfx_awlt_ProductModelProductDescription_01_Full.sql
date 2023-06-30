-- Databricks notebook source
-- Full load notebook bfx_awlt_ProductModelProductDescription
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription`;
CREATE TABLE IF NOT EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription`;

COPY INTO `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription`
FROM
(
    SELECT   CAST('awlt'||'~'||UPPER(COALESCE(TRIM(STRING(`ProductModelID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(`ProductDescriptionID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(`Culture`)),'NVL')) AS STRING) AS `ProductModelProductDescription_BK`
			,CAST(`ProductModelID` AS INT) AS `ProductModelID`
			,CAST(`ProductDescriptionID` AS INT) AS `ProductDescriptionID`
			,CAST(`Culture` AS STRING) AS `Culture`
			,CAST(`rowguid` AS STRING) AS `rowguid`
			,TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `ModifiedDate`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
			,SHA1(STRING(
				COALESCE(TRIM(UPPER(REPLACE(REPLACE(STRING(`rowguid`), '{', ''), '}', ''))), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL'))) AS `FlexRowHash`
    FROM    '${location_path}/AdventureWorksLT2012_SalesLT_ProductModelProductDescription'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_DELTA`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_DELTA` AS
SELECT	 STG.`ProductModelProductDescription_BK`
		,STG.`ProductModelID`
		,STG.`ProductDescriptionID`
		,STG.`Culture`
		,STG.`rowguid`
		,STG.`ModifiedDate`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowChangeType`
		,STG.`FlexRowRecordSource`
		,STG.`FlexRowHash`
FROM
(
	SELECT	 SRC.`ProductModelProductDescription_BK`
			,SRC.`ProductModelID`
			,SRC.`ProductDescriptionID`
			,SRC.`Culture`
			,SRC.`rowguid`
			,SRC.`ModifiedDate`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowChangeType`
			,SRC.`FlexRowRecordSource`
			,SRC.`FlexRowHash`
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`ProductModelID`,SRC.`ProductDescriptionID`,SRC.`Culture`,SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`ProductModelID`,SRC.`ProductDescriptionID`,SRC.`Culture`, SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`ProductModelID`,SRC.`ProductDescriptionID`,SRC.`Culture` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`ProductModelID`,SRC.`ProductDescriptionID`,SRC.`Culture` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription` AS SRC
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`ProductModelProductDescription`;
INSERT INTO `${_bfx_stg}`.`awlt`.`ProductModelProductDescription`
        (`ProductModelProductDescription_BK`
		,`ProductModelID`
		,`ProductDescriptionID`
		,`Culture`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT   SRC.`ProductModelProductDescription_BK`
		,SRC.`ProductModelID`
		,SRC.`ProductDescriptionID`
		,SRC.`Culture`
		,SRC.`rowguid`
		,SRC.`ModifiedDate`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,SRC.`FlexRowChangeType`
		,SRC.`FlexRowRecordSource`
		,SRC.`FlexRowHash`
FROM	`TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_DELTA` SRC;

-- COMMAND ----------
INSERT INTO `${_bfx_ods}`.`awlt`.`ProductModelProductDescription`
        (`ProductModelID`
		,`ProductDescriptionID`
		,`Culture`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT   INS.`ProductModelID`
		,INS.`ProductDescriptionID`
		,INS.`Culture`
		,INS.`rowguid`
		,INS.`ModifiedDate`
		,INS.`FlexRowEffectiveFromDate`
		,INS.`FlexRowAuditId`
		,INS.`FlexRowChangeType`
		,INS.`FlexRowRecordSource`
		,INS.`FlexRowHash`
FROM	`${_bfx_stg}`.`awlt`.`ProductModelProductDescription` INS;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_DELTA`;

