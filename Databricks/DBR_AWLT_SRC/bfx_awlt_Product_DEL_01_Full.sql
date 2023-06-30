-- Databricks notebook source
-- Delete Detection notebook bfx_awlt_Product_DEL
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";



--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_Product_DEL_LND`;

CREATE TABLE IF NOT EXISTS `TMP_AWLT_02_Sales_01_Product_DEL_LND`;
COPY INTO `TMP_AWLT_02_Sales_01_Product_DEL_LND`
FROM
(
    SELECT   CAST(`ProductID` AS INT) AS `ProductID`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/AdventureWorksLT2012_awlt_Product_DEL'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_Product_DEL_CACHE`;

CREATE TABLE `TMP_AWLT_02_Sales_01_Product_DEL_CACHE` AS
SELECT	 PSA.`ProductID` AS `ProductID`
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}`.`awlt`.`Product` PSA
INNER JOIN
(	SELECT	 STG.`ProductID`
			,MAX(STG.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`${_bfx_ods}`.`awlt`.`Product` STG
	GROUP BY STG.`ProductID`
) GRP
	ON	PSA.`ProductID` = GRP.`ProductID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D';
--COMMAND----------

TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`Product_DEL`;

INSERT INTO `${_bfx_stg}`.`awlt`.`Product_DEL`
		(`ProductID`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT	 CAST(PSA.`ProductID` AS INT) AS `ProductID`
		,FLX.`FlexRowEffectiveFromDate`
		,FLX.`FlexRowAuditId`
		,FLX.`FlexRowRecordSource`
FROM	`TMP_AWLT_02_Sales_01_Product_DEL_CACHE` PSA
LEFT OUTER JOIN `TMP_AWLT_02_Sales_01_Product_DEL_LND` LND
	ON	PSA.`ProductID` = LND.`ProductID`
CROSS JOIN
(	SELECT	 TOP 1 `FlexRowEffectiveFromDate`
			,`FlexRowAuditId`
			,`FlexRowRecordSource`
	FROM	`TMP_AWLT_02_Sales_01_Product_DEL_LND`
) AS FLX
WHERE	LND.`ProductID` IS NULL

--COMMAND----------

INSERT INTO `${_bfx_ods}`.`awlt`.`Product`
		(`ProductID`
		,`Name`
		,`ProductNumber`
		,`Color`
		,`StandardCost`
		,`ListPrice`
		,`Size`
		,`Weight`
		,`ProductCategoryID`
		,`ProductModelID`
		,`SellStartDate`
		,`SellEndDate`
		,`DiscontinuedDate`
		,`ThumbnailPhotoFileName`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT	 PSA.`ProductID`
		,PSA.`Name`
		,PSA.`ProductNumber`
		,PSA.`Color`
		,PSA.`StandardCost`
		,PSA.`ListPrice`
		,PSA.`Size`
		,PSA.`Weight`
		,PSA.`ProductCategoryID`
		,PSA.`ProductModelID`
		,PSA.`SellStartDate`
		,PSA.`SellEndDate`
		,PSA.`DiscontinuedDate`
		,PSA.`ThumbnailPhotoFileName`
		,PSA.`rowguid`
		,PSA.`ModifiedDate`
		,DEL.`FlexRowEffectiveFromDate`
		,DEL.`FlexRowAuditId`
		,CAST('D' AS CHAR(1)) AS `FlexRowChangeType`
		,DEL.`FlexRowRecordSource`
		,PSA.`FlexRowHash`
FROM	`${_bfx_ods}`.`awlt`.`Product` PSA
INNER JOIN `TMP_AWLT_02_Sales_01_Product_DEL_CACHE` GRP
	ON	PSA.`ProductID` = GRP.`ProductID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D'
INNER JOIN `${_bfx_stg}`.`awlt`.`Product_DEL` DEL
	ON	DEL.`ProductID` = PSA.`ProductID`;

--COMMAND----------
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_Product_DEL_LND`;
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_Product_DEL_CACHE`;