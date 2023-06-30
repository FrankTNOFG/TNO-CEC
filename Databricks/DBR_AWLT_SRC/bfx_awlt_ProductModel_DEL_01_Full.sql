-- Databricks notebook source
-- Delete Detection notebook bfx_awlt_ProductModel_DEL
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";



--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductModel_DEL_LND`;

CREATE TABLE IF NOT EXISTS `TMP_AWLT_02_Sales_01_ProductModel_DEL_LND`;
COPY INTO `TMP_AWLT_02_Sales_01_ProductModel_DEL_LND`
FROM
(
    SELECT   CAST(`ProductModelID` AS INT) AS `ProductModelID`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/@concat(toLower(pipeline().parameters.SourceScopedName), '/', substring(pipeline().parameters.BatchStartTime,0,4))'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductModel_DEL_CACHE`;

CREATE TABLE `TMP_AWLT_02_Sales_01_ProductModel_DEL_CACHE` AS
SELECT	 PSA.`ProductModelID` AS `ProductModelID`
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}`.`awlt`.`ProductModel` PSA
INNER JOIN
(	SELECT	 STG.`ProductModelID`
			,MAX(STG.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`${_bfx_ods}`.`awlt`.`ProductModel` STG
	GROUP BY STG.`ProductModelID`
) GRP
	ON	PSA.`ProductModelID` = GRP.`ProductModelID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D';
--COMMAND----------

TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`ProductModel_DEL`;

INSERT INTO `${_bfx_stg}`.`awlt`.`ProductModel_DEL`
		(`ProductModelID`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT	 CAST(PSA.`ProductModelID` AS INT) AS `ProductModelID`
		,FLX.`FlexRowEffectiveFromDate`
		,FLX.`FlexRowAuditId`
		,FLX.`FlexRowRecordSource`
FROM	`TMP_AWLT_02_Sales_01_ProductModel_DEL_CACHE` PSA
LEFT OUTER JOIN `TMP_AWLT_02_Sales_01_ProductModel_DEL_LND` LND
	ON	PSA.`ProductModelID` = LND.`ProductModelID`
CROSS JOIN
(	SELECT	 TOP 1 `FlexRowEffectiveFromDate`
			,`FlexRowAuditId`
			,`FlexRowRecordSource`
	FROM	`TMP_AWLT_02_Sales_01_ProductModel_DEL_LND`
) AS FLX
WHERE	LND.`ProductModelID` IS NULL

--COMMAND----------

INSERT INTO `${_bfx_ods}`.`awlt`.`ProductModel`
		(`ProductModelID`
		,`Name`
		,`CatalogDescription`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT	 PSA.`ProductModelID`
		,PSA.`Name`
		,PSA.`CatalogDescription`
		,PSA.`rowguid`
		,PSA.`ModifiedDate`
		,DEL.`FlexRowEffectiveFromDate`
		,DEL.`FlexRowAuditId`
		,CAST('D' AS CHAR(1)) AS `FlexRowChangeType`
		,DEL.`FlexRowRecordSource`
		,PSA.`FlexRowHash`
FROM	`${_bfx_ods}`.`awlt`.`ProductModel` PSA
INNER JOIN `TMP_AWLT_02_Sales_01_ProductModel_DEL_CACHE` GRP
	ON	PSA.`ProductModelID` = GRP.`ProductModelID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D'
INNER JOIN `${_bfx_stg}`.`awlt`.`ProductModel_DEL` DEL
	ON	DEL.`ProductModelID` = PSA.`ProductModelID`;

--COMMAND----------
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductModel_DEL_LND`;
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductModel_DEL_CACHE`;