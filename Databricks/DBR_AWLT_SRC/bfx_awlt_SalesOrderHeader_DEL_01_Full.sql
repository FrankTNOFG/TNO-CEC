-- Databricks notebook source
-- Delete Detection notebook bfx_awlt_SalesOrderHeader_DEL
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";



--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_SalesOrderHeade_001_DEL_LND`;

CREATE TABLE IF NOT EXISTS `TMP_AWLT_02_Sales_01_SalesOrderHeade_001_DEL_LND`;
COPY INTO `TMP_AWLT_02_Sales_01_SalesOrderHeade_001_DEL_LND`
FROM
(
    SELECT   CAST(`SalesOrderID` AS INT) AS `SalesOrderID`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/AdventureWorksLT2012_awlt_SalesOrderHeader_DEL'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_SalesOrderHeade_001_DEL_CACHE`;

CREATE TABLE `TMP_AWLT_02_Sales_01_SalesOrderHeade_001_DEL_CACHE` AS
SELECT	 PSA.`SalesOrderID` AS `SalesOrderID`
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}`.`awlt`.`SalesOrderHeader` PSA
INNER JOIN
(	SELECT	 STG.`SalesOrderID`
			,MAX(STG.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`${_bfx_ods}`.`awlt`.`SalesOrderHeader` STG
	GROUP BY STG.`SalesOrderID`
) GRP
	ON	PSA.`SalesOrderID` = GRP.`SalesOrderID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D';
--COMMAND----------

TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`SalesOrderHeader_DEL`;

INSERT INTO `${_bfx_stg}`.`awlt`.`SalesOrderHeader_DEL`
		(`SalesOrderID`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT	 CAST(PSA.`SalesOrderID` AS INT) AS `SalesOrderID`
		,FLX.`FlexRowEffectiveFromDate`
		,FLX.`FlexRowAuditId`
		,FLX.`FlexRowRecordSource`
FROM	`TMP_AWLT_02_Sales_01_SalesOrderHeade_001_DEL_CACHE` PSA
LEFT OUTER JOIN `TMP_AWLT_02_Sales_01_SalesOrderHeade_001_DEL_LND` LND
	ON	PSA.`SalesOrderID` = LND.`SalesOrderID`
CROSS JOIN
(	SELECT	 TOP 1 `FlexRowEffectiveFromDate`
			,`FlexRowAuditId`
			,`FlexRowRecordSource`
	FROM	`TMP_AWLT_02_Sales_01_SalesOrderHeade_001_DEL_LND`
) AS FLX
WHERE	LND.`SalesOrderID` IS NULL

--COMMAND----------

INSERT INTO `${_bfx_ods}`.`awlt`.`SalesOrderHeader`
		(`SalesOrderID`
		,`RevisionNumber`
		,`OrderDate`
		,`DueDate`
		,`ShipDate`
		,`Status`
		,`OnlineOrderFlag`
		,`SalesOrderNumber`
		,`PurchaseOrderNumber`
		,`AccountNumber`
		,`CustomerID`
		,`ShipToAddressID`
		,`BillToAddressID`
		,`ShipMethod`
		,`CreditCardApprovalCode`
		,`SubTotal`
		,`TaxAmt`
		,`Freight`
		,`TotalDue`
		,`Comment`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT	 PSA.`SalesOrderID`
		,PSA.`RevisionNumber`
		,PSA.`OrderDate`
		,PSA.`DueDate`
		,PSA.`ShipDate`
		,PSA.`Status`
		,PSA.`OnlineOrderFlag`
		,PSA.`SalesOrderNumber`
		,PSA.`PurchaseOrderNumber`
		,PSA.`AccountNumber`
		,PSA.`CustomerID`
		,PSA.`ShipToAddressID`
		,PSA.`BillToAddressID`
		,PSA.`ShipMethod`
		,PSA.`CreditCardApprovalCode`
		,PSA.`SubTotal`
		,PSA.`TaxAmt`
		,PSA.`Freight`
		,PSA.`TotalDue`
		,PSA.`Comment`
		,PSA.`rowguid`
		,PSA.`ModifiedDate`
		,DEL.`FlexRowEffectiveFromDate`
		,DEL.`FlexRowAuditId`
		,CAST('D' AS CHAR(1)) AS `FlexRowChangeType`
		,DEL.`FlexRowRecordSource`
		,PSA.`FlexRowHash`
FROM	`${_bfx_ods}`.`awlt`.`SalesOrderHeader` PSA
INNER JOIN `TMP_AWLT_02_Sales_01_SalesOrderHeade_001_DEL_CACHE` GRP
	ON	PSA.`SalesOrderID` = GRP.`SalesOrderID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D'
INNER JOIN `${_bfx_stg}`.`awlt`.`SalesOrderHeader_DEL` DEL
	ON	DEL.`SalesOrderID` = PSA.`SalesOrderID`;

--COMMAND----------
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_SalesOrderHeade_001_DEL_LND`;
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_SalesOrderHeade_001_DEL_CACHE`;