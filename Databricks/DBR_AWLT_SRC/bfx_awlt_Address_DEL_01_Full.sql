-- Databricks notebook source
-- Delete Detection notebook bfx_awlt_Address_DEL
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";



--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_Address_DEL_LND`;

CREATE TABLE IF NOT EXISTS `TMP_AWLT_02_Sales_01_Address_DEL_LND`;
COPY INTO `TMP_AWLT_02_Sales_01_Address_DEL_LND`
FROM
(
    SELECT   CAST(`AddressID` AS INT) AS `AddressID`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/AdventureWorksLT2012_awlt_Address_DEL'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_Address_DEL_CACHE`;

CREATE TABLE `TMP_AWLT_02_Sales_01_Address_DEL_CACHE` AS
SELECT	 PSA.`AddressID` AS `AddressID`
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}`.`awlt`.`Address` PSA
INNER JOIN
(	SELECT	 STG.`AddressID`
			,MAX(STG.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`${_bfx_ods}`.`awlt`.`Address` STG
	GROUP BY STG.`AddressID`
) GRP
	ON	PSA.`AddressID` = GRP.`AddressID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D';
--COMMAND----------

TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`Address_DEL`;

INSERT INTO `${_bfx_stg}`.`awlt`.`Address_DEL`
		(`AddressID`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT	 CAST(PSA.`AddressID` AS INT) AS `AddressID`
		,FLX.`FlexRowEffectiveFromDate`
		,FLX.`FlexRowAuditId`
		,FLX.`FlexRowRecordSource`
FROM	`TMP_AWLT_02_Sales_01_Address_DEL_CACHE` PSA
LEFT OUTER JOIN `TMP_AWLT_02_Sales_01_Address_DEL_LND` LND
	ON	PSA.`AddressID` = LND.`AddressID`
CROSS JOIN
(	SELECT	 TOP 1 `FlexRowEffectiveFromDate`
			,`FlexRowAuditId`
			,`FlexRowRecordSource`
	FROM	`TMP_AWLT_02_Sales_01_Address_DEL_LND`
) AS FLX
WHERE	LND.`AddressID` IS NULL

--COMMAND----------

INSERT INTO `${_bfx_ods}`.`awlt`.`Address`
		(`AddressID`
		,`AddressLine1`
		,`AddressLine2`
		,`City`
		,`StateProvince`
		,`CountryRegion`
		,`PostalCode`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT	 PSA.`AddressID`
		,PSA.`AddressLine1`
		,PSA.`AddressLine2`
		,PSA.`City`
		,PSA.`StateProvince`
		,PSA.`CountryRegion`
		,PSA.`PostalCode`
		,PSA.`rowguid`
		,PSA.`ModifiedDate`
		,DEL.`FlexRowEffectiveFromDate`
		,DEL.`FlexRowAuditId`
		,CAST('D' AS CHAR(1)) AS `FlexRowChangeType`
		,DEL.`FlexRowRecordSource`
		,PSA.`FlexRowHash`
FROM	`${_bfx_ods}`.`awlt`.`Address` PSA
INNER JOIN `TMP_AWLT_02_Sales_01_Address_DEL_CACHE` GRP
	ON	PSA.`AddressID` = GRP.`AddressID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D'
INNER JOIN `${_bfx_stg}`.`awlt`.`Address_DEL` DEL
	ON	DEL.`AddressID` = PSA.`AddressID`;

--COMMAND----------
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_Address_DEL_LND`;
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_Address_DEL_CACHE`;