-- Databricks notebook source
-- Delete Detection notebook bfx_awlt_SalesOrderDetail_DEL
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";



--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_SalesOrderDetai_001_DEL_LND`;

CREATE TABLE IF NOT EXISTS `TMP_AWLT_02_Sales_01_SalesOrderDetai_001_DEL_LND`;
COPY INTO `TMP_AWLT_02_Sales_01_SalesOrderDetai_001_DEL_LND`
FROM
(
    SELECT   CAST('awlt'||'~'||UPPER(COALESCE(TRIM(STRING(`SalesOrderID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(`SalesOrderDetailID`)),'NVL')) AS STRING) AS `SalesOrderDetail_BK`
			,CAST(`SalesOrderID` AS INT) AS `SalesOrderID`
			,CAST(`SalesOrderDetailID` AS INT) AS `SalesOrderDetailID`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/AdventureWorksLT2012_awlt_SalesOrderDetail_DEL'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_SalesOrderDetai_001_DEL_CACHE`;

CREATE TABLE `TMP_AWLT_02_Sales_01_SalesOrderDetai_001_DEL_CACHE` AS
SELECT	 CAST('awlt'||'~'||UPPER(COALESCE(TRIM(STRING(PSA.`SalesOrderID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(PSA.`SalesOrderDetailID`)),'NVL')) AS STRING) AS `SalesOrderDetail_BK`
		,PSA.`SalesOrderID` AS `SalesOrderID`
		,PSA.`SalesOrderDetailID` AS `SalesOrderDetailID`
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}`.`awlt`.`SalesOrderDetail` PSA
INNER JOIN
(	SELECT	 STG.`SalesOrderID`
			,STG.`SalesOrderDetailID`
			,MAX(STG.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`${_bfx_ods}`.`awlt`.`SalesOrderDetail` STG
	GROUP BY STG.`SalesOrderID`
			,STG.`SalesOrderDetailID`
) GRP
	ON	PSA.`SalesOrderID` = GRP.`SalesOrderID`
	AND	PSA.`SalesOrderDetailID` = GRP.`SalesOrderDetailID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D';
--COMMAND----------

TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`SalesOrderDetail_DEL`;

INSERT INTO `${_bfx_stg}`.`awlt`.`SalesOrderDetail_DEL`
		(`SalesOrderDetail_BK`
		,`SalesOrderID`
		,`SalesOrderDetailID`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT	 CAST(PSA.`SalesOrderDetail_BK` AS STRING) AS `SalesOrderDetail_BK`
		,CAST(PSA.`SalesOrderID` AS INT) AS `SalesOrderID`
		,CAST(PSA.`SalesOrderDetailID` AS INT) AS `SalesOrderDetailID`
		,FLX.`FlexRowEffectiveFromDate`
		,FLX.`FlexRowAuditId`
		,FLX.`FlexRowRecordSource`
FROM	`TMP_AWLT_02_Sales_01_SalesOrderDetai_001_DEL_CACHE` PSA
LEFT OUTER JOIN `TMP_AWLT_02_Sales_01_SalesOrderDetai_001_DEL_LND` LND
	ON	PSA.`SalesOrderDetail_BK` = LND.`SalesOrderDetail_BK`
CROSS JOIN
(	SELECT	 TOP 1 `FlexRowEffectiveFromDate`
			,`FlexRowAuditId`
			,`FlexRowRecordSource`
	FROM	`TMP_AWLT_02_Sales_01_SalesOrderDetai_001_DEL_LND`
) AS FLX
WHERE	LND.`SalesOrderDetail_BK` IS NULL

--COMMAND----------

INSERT INTO `${_bfx_ods}`.`awlt`.`SalesOrderDetail`
		(`SalesOrderID`
		,`SalesOrderDetailID`
		,`OrderQty`
		,`ProductID`
		,`UnitPrice`
		,`UnitPriceDiscount`
		,`LineTotal`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT	 PSA.`SalesOrderID`
		,PSA.`SalesOrderDetailID`
		,PSA.`OrderQty`
		,PSA.`ProductID`
		,PSA.`UnitPrice`
		,PSA.`UnitPriceDiscount`
		,PSA.`LineTotal`
		,PSA.`rowguid`
		,PSA.`ModifiedDate`
		,DEL.`FlexRowEffectiveFromDate`
		,DEL.`FlexRowAuditId`
		,CAST('D' AS CHAR(1)) AS `FlexRowChangeType`
		,DEL.`FlexRowRecordSource`
		,PSA.`FlexRowHash`
FROM	`${_bfx_ods}`.`awlt`.`SalesOrderDetail` PSA
INNER JOIN `TMP_AWLT_02_Sales_01_SalesOrderDetai_001_DEL_CACHE` GRP
	ON	PSA.`SalesOrderID` = GRP.`SalesOrderID`
	AND	PSA.`SalesOrderDetailID` = GRP.`SalesOrderDetailID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D'
INNER JOIN `${_bfx_stg}`.`awlt`.`SalesOrderDetail_DEL` DEL
	ON	DEL.`SalesOrderID` = PSA.`SalesOrderID`
	AND	DEL.`SalesOrderDetailID` = PSA.`SalesOrderDetailID`;

--COMMAND----------
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_SalesOrderDetai_001_DEL_LND`;
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_SalesOrderDetai_001_DEL_CACHE`;