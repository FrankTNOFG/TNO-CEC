-- Databricks notebook source
-- Delete Detection notebook bfx_awlt_Customer_DEL
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";



--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_Customer_DEL_LND`;

CREATE TABLE IF NOT EXISTS `TMP_AWLT_02_Sales_01_Customer_DEL_LND`;
COPY INTO `TMP_AWLT_02_Sales_01_Customer_DEL_LND`
FROM
(
    SELECT   CAST(`CustomerID` AS INT) AS `CustomerID`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/AdventureWorksLT2012_awlt_Customer_DEL'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_Customer_DEL_CACHE`;

CREATE TABLE `TMP_AWLT_02_Sales_01_Customer_DEL_CACHE` AS
SELECT	 PSA.`CustomerID` AS `CustomerID`
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}`.`awlt`.`Customer` PSA
INNER JOIN
(	SELECT	 STG.`CustomerID`
			,MAX(STG.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`${_bfx_ods}`.`awlt`.`Customer` STG
	GROUP BY STG.`CustomerID`
) GRP
	ON	PSA.`CustomerID` = GRP.`CustomerID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D';
--COMMAND----------

TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`Customer_DEL`;

INSERT INTO `${_bfx_stg}`.`awlt`.`Customer_DEL`
		(`CustomerID`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT	 PSA.`CustomerID` AS `CustomerID`
		,FLX.`FlexRowEffectiveFromDate`
		,FLX.`FlexRowAuditId`
		,FLX.`FlexRowRecordSource`
FROM	`TMP_AWLT_02_Sales_01_Customer_DEL_CACHE` PSA
LEFT OUTER JOIN `TMP_AWLT_02_Sales_01_Customer_DEL_LND` LND
	ON	PSA.`CustomerID` = LND.`CustomerID`
CROSS JOIN
(	SELECT	 TOP 1 `FlexRowEffectiveFromDate`
			,`FlexRowAuditId`
			,`FlexRowRecordSource`
	FROM	`TMP_AWLT_02_Sales_01_Customer_DEL_LND`
) AS FLX
WHERE	LND.`CustomerID` IS NULL

--COMMAND----------

INSERT INTO `${_bfx_ods}`.`awlt`.`Customer`
		(`CustomerID`
		,`NameStyle`
		,`Title`
		,`FirstName`
		,`MiddleName`
		,`LastName`
		,`Suffix`
		,`CompanyName`
		,`SalesPerson`
		,`EmailAddress`
		,`Phone`
		,`PasswordHash`
		,`PasswordSalt`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT	 PSA.`CustomerID`
		,PSA.`NameStyle`
		,PSA.`Title`
		,PSA.`FirstName`
		,PSA.`MiddleName`
		,PSA.`LastName`
		,PSA.`Suffix`
		,PSA.`CompanyName`
		,PSA.`SalesPerson`
		,PSA.`EmailAddress`
		,PSA.`Phone`
		,PSA.`PasswordHash`
		,PSA.`PasswordSalt`
		,PSA.`rowguid`
		,PSA.`ModifiedDate`
		,DEL.`FlexRowEffectiveFromDate`
		,DEL.`FlexRowAuditId`
		,CAST('D' AS CHAR(1)) AS `FlexRowChangeType`
		,DEL.`FlexRowRecordSource`
		,PSA.`FlexRowHash`
FROM	`${_bfx_ods}`.`awlt`.`Customer` PSA
INNER JOIN `TMP_AWLT_02_Sales_01_Customer_DEL_CACHE` GRP
	ON	PSA.`CustomerID` = GRP.`CustomerID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D'
INNER JOIN `${_bfx_stg}`.`awlt`.`Customer_DEL` DEL
	ON	DEL.`CustomerID` = PSA.`CustomerID`;

--COMMAND----------
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_Customer_DEL_LND`;
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_Customer_DEL_CACHE`;