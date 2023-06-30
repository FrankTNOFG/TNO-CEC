-- Databricks notebook source
-- Delete Detection notebook bfx_awlt_ProductCategory_DEL
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";



--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductCategory_DEL_LND`;

CREATE TABLE IF NOT EXISTS `TMP_AWLT_02_Sales_01_ProductCategory_DEL_LND`;
COPY INTO `TMP_AWLT_02_Sales_01_ProductCategory_DEL_LND`
FROM
(
    SELECT   CAST(`ProductCategoryID` AS INT) AS `ProductCategoryID`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/AdventureWorksLT2012_awlt_ProductCategory_DEL'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductCategory_DEL_CACHE`;

CREATE TABLE `TMP_AWLT_02_Sales_01_ProductCategory_DEL_CACHE` AS
SELECT	 PSA.`ProductCategoryID` AS `ProductCategoryID`
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}`.`awlt`.`ProductCategory` PSA
INNER JOIN
(	SELECT	 STG.`ProductCategoryID`
			,MAX(STG.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`${_bfx_ods}`.`awlt`.`ProductCategory` STG
	GROUP BY STG.`ProductCategoryID`
) GRP
	ON	PSA.`ProductCategoryID` = GRP.`ProductCategoryID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D';
--COMMAND----------

TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`ProductCategory_DEL`;

INSERT INTO `${_bfx_stg}`.`awlt`.`ProductCategory_DEL`
		(`ProductCategoryID`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT	 CAST(PSA.`ProductCategoryID` AS INT) AS `ProductCategoryID`
		,FLX.`FlexRowEffectiveFromDate`
		,FLX.`FlexRowAuditId`
		,FLX.`FlexRowRecordSource`
FROM	`TMP_AWLT_02_Sales_01_ProductCategory_DEL_CACHE` PSA
LEFT OUTER JOIN `TMP_AWLT_02_Sales_01_ProductCategory_DEL_LND` LND
	ON	PSA.`ProductCategoryID` = LND.`ProductCategoryID`
CROSS JOIN
(	SELECT	 TOP 1 `FlexRowEffectiveFromDate`
			,`FlexRowAuditId`
			,`FlexRowRecordSource`
	FROM	`TMP_AWLT_02_Sales_01_ProductCategory_DEL_LND`
) AS FLX
WHERE	LND.`ProductCategoryID` IS NULL

--COMMAND----------

INSERT INTO `${_bfx_ods}`.`awlt`.`ProductCategory`
		(`ProductCategoryID`
		,`ParentProductCategoryID`
		,`Name`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT	 PSA.`ProductCategoryID`
		,PSA.`ParentProductCategoryID`
		,PSA.`Name`
		,PSA.`rowguid`
		,PSA.`ModifiedDate`
		,DEL.`FlexRowEffectiveFromDate`
		,DEL.`FlexRowAuditId`
		,CAST('D' AS CHAR(1)) AS `FlexRowChangeType`
		,DEL.`FlexRowRecordSource`
		,PSA.`FlexRowHash`
FROM	`${_bfx_ods}`.`awlt`.`ProductCategory` PSA
INNER JOIN `TMP_AWLT_02_Sales_01_ProductCategory_DEL_CACHE` GRP
	ON	PSA.`ProductCategoryID` = GRP.`ProductCategoryID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D'
INNER JOIN `${_bfx_stg}`.`awlt`.`ProductCategory_DEL` DEL
	ON	DEL.`ProductCategoryID` = PSA.`ProductCategoryID`;

--COMMAND----------
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductCategory_DEL_LND`;
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductCategory_DEL_CACHE`;