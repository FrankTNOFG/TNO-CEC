-- Databricks notebook source
-- Delete Detection notebook bfx_awlt_ProductDescription_DEL
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";



--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductDescript_001_DEL_LND`;

CREATE TABLE IF NOT EXISTS `TMP_AWLT_02_Sales_01_ProductDescript_001_DEL_LND`;
COPY INTO `TMP_AWLT_02_Sales_01_ProductDescript_001_DEL_LND`
FROM
(
    SELECT   CAST(`ProductDescriptionID` AS INT) AS `ProductDescriptionID`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/AdventureWorksLT2012_awlt_ProductDescription_DEL'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductDescript_001_DEL_CACHE`;

CREATE TABLE `TMP_AWLT_02_Sales_01_ProductDescript_001_DEL_CACHE` AS
SELECT	 PSA.`ProductDescriptionID` AS `ProductDescriptionID`
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}`.`awlt`.`ProductDescription` PSA
INNER JOIN
(	SELECT	 STG.`ProductDescriptionID`
			,MAX(STG.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`${_bfx_ods}`.`awlt`.`ProductDescription` STG
	GROUP BY STG.`ProductDescriptionID`
) GRP
	ON	PSA.`ProductDescriptionID` = GRP.`ProductDescriptionID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D';
--COMMAND----------

TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`ProductDescription_DEL`;

INSERT INTO `${_bfx_stg}`.`awlt`.`ProductDescription_DEL`
		(`ProductDescriptionID`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT	 CAST(PSA.`ProductDescriptionID` AS INT) AS `ProductDescriptionID`
		,FLX.`FlexRowEffectiveFromDate`
		,FLX.`FlexRowAuditId`
		,FLX.`FlexRowRecordSource`
FROM	`TMP_AWLT_02_Sales_01_ProductDescript_001_DEL_CACHE` PSA
LEFT OUTER JOIN `TMP_AWLT_02_Sales_01_ProductDescript_001_DEL_LND` LND
	ON	PSA.`ProductDescriptionID` = LND.`ProductDescriptionID`
CROSS JOIN
(	SELECT	 TOP 1 `FlexRowEffectiveFromDate`
			,`FlexRowAuditId`
			,`FlexRowRecordSource`
	FROM	`TMP_AWLT_02_Sales_01_ProductDescript_001_DEL_LND`
) AS FLX
WHERE	LND.`ProductDescriptionID` IS NULL

--COMMAND----------

INSERT INTO `${_bfx_ods}`.`awlt`.`ProductDescription`
		(`ProductDescriptionID`
		,`Description`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT	 PSA.`ProductDescriptionID`
		,PSA.`Description`
		,PSA.`rowguid`
		,PSA.`ModifiedDate`
		,DEL.`FlexRowEffectiveFromDate`
		,DEL.`FlexRowAuditId`
		,CAST('D' AS CHAR(1)) AS `FlexRowChangeType`
		,DEL.`FlexRowRecordSource`
		,PSA.`FlexRowHash`
FROM	`${_bfx_ods}`.`awlt`.`ProductDescription` PSA
INNER JOIN `TMP_AWLT_02_Sales_01_ProductDescript_001_DEL_CACHE` GRP
	ON	PSA.`ProductDescriptionID` = GRP.`ProductDescriptionID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D'
INNER JOIN `${_bfx_stg}`.`awlt`.`ProductDescription_DEL` DEL
	ON	DEL.`ProductDescriptionID` = PSA.`ProductDescriptionID`;

--COMMAND----------
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductDescript_001_DEL_LND`;
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductDescript_001_DEL_CACHE`;