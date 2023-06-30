-- Databricks notebook source
-- Delete Detection notebook bfx_awlt_CustomerAddress_DEL
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";



--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_CustomerAddress_DEL_LND`;

CREATE TABLE IF NOT EXISTS `TMP_AWLT_02_Sales_01_CustomerAddress_DEL_LND`;
COPY INTO `TMP_AWLT_02_Sales_01_CustomerAddress_DEL_LND`
FROM
(
    SELECT   CAST('awlt'||'~'||UPPER(COALESCE(TRIM(STRING(`CustomerID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(`AddressID`)),'NVL')) AS STRING) AS `CustomerAddress_BK`
			,CAST(`CustomerID` AS INT) AS `CustomerID`
			,CAST(`AddressID` AS INT) AS `AddressID`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/AdventureWorksLT2012_awlt_CustomerAddress_DEL'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_CustomerAddress_DEL_CACHE`;

CREATE TABLE `TMP_AWLT_02_Sales_01_CustomerAddress_DEL_CACHE` AS
SELECT	 CAST('awlt'||'~'||UPPER(COALESCE(TRIM(STRING(PSA.`CustomerID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(PSA.`AddressID`)),'NVL')) AS STRING) AS `CustomerAddress_BK`
		,PSA.`CustomerID` AS `CustomerID`
		,PSA.`AddressID` AS `AddressID`
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}`.`awlt`.`CustomerAddress` PSA
INNER JOIN
(	SELECT	 STG.`CustomerID`
			,STG.`AddressID`
			,MAX(STG.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`${_bfx_ods}`.`awlt`.`CustomerAddress` STG
	GROUP BY STG.`CustomerID`
			,STG.`AddressID`
) GRP
	ON	PSA.`CustomerID` = GRP.`CustomerID`
	AND	PSA.`AddressID` = GRP.`AddressID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D';
--COMMAND----------

TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`CustomerAddress_DEL`;

INSERT INTO `${_bfx_stg}`.`awlt`.`CustomerAddress_DEL`
		(`CustomerAddress_BK`
		,`CustomerID`
		,`AddressID`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT	 CAST(PSA.`CustomerAddress_BK` AS STRING) AS `CustomerAddress_BK`
		,CAST(PSA.`CustomerID` AS INT) AS `CustomerID`
		,CAST(PSA.`AddressID` AS INT) AS `AddressID`
		,FLX.`FlexRowEffectiveFromDate`
		,FLX.`FlexRowAuditId`
		,FLX.`FlexRowRecordSource`
FROM	`TMP_AWLT_02_Sales_01_CustomerAddress_DEL_CACHE` PSA
LEFT OUTER JOIN `TMP_AWLT_02_Sales_01_CustomerAddress_DEL_LND` LND
	ON	PSA.`CustomerAddress_BK` = LND.`CustomerAddress_BK`
CROSS JOIN
(	SELECT	 TOP 1 `FlexRowEffectiveFromDate`
			,`FlexRowAuditId`
			,`FlexRowRecordSource`
	FROM	`TMP_AWLT_02_Sales_01_CustomerAddress_DEL_LND`
) AS FLX
WHERE	LND.`CustomerAddress_BK` IS NULL

--COMMAND----------

INSERT INTO `${_bfx_ods}`.`awlt`.`CustomerAddress`
		(`CustomerID`
		,`AddressID`
		,`AddressType`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT	 PSA.`CustomerID`
		,PSA.`AddressID`
		,PSA.`AddressType`
		,PSA.`rowguid`
		,PSA.`ModifiedDate`
		,DEL.`FlexRowEffectiveFromDate`
		,DEL.`FlexRowAuditId`
		,CAST('D' AS CHAR(1)) AS `FlexRowChangeType`
		,DEL.`FlexRowRecordSource`
		,PSA.`FlexRowHash`
FROM	`${_bfx_ods}`.`awlt`.`CustomerAddress` PSA
INNER JOIN `TMP_AWLT_02_Sales_01_CustomerAddress_DEL_CACHE` GRP
	ON	PSA.`CustomerID` = GRP.`CustomerID`
	AND	PSA.`AddressID` = GRP.`AddressID`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D'
INNER JOIN `${_bfx_stg}`.`awlt`.`CustomerAddress_DEL` DEL
	ON	DEL.`CustomerID` = PSA.`CustomerID`
	AND	DEL.`AddressID` = PSA.`AddressID`;

--COMMAND----------
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_CustomerAddress_DEL_LND`;
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_CustomerAddress_DEL_CACHE`;