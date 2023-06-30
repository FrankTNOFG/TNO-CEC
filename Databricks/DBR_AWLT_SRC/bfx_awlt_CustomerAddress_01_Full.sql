-- Databricks notebook source
-- Full load notebook bfx_awlt_CustomerAddress
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_CustomerAddress`;
CREATE TABLE IF NOT EXISTS `TMP_AdventureWorksLT2012_SalesLT_CustomerAddress`;

COPY INTO `TMP_AdventureWorksLT2012_SalesLT_CustomerAddress`
FROM
(
    SELECT   CAST('awlt'||'~'||UPPER(COALESCE(TRIM(STRING(`CustomerID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(`AddressID`)),'NVL')) AS STRING) AS `CustomerAddress_BK`
			,CAST(`CustomerID` AS INT) AS `CustomerID`
			,CAST(`AddressID` AS INT) AS `AddressID`
			,CAST(`AddressType` AS STRING) AS `AddressType`
			,CAST(`rowguid` AS STRING) AS `rowguid`
			,TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `ModifiedDate`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
			,SHA1(STRING(
				COALESCE(TRIM(`AddressType`), 'NVL')||'~'||
				COALESCE(TRIM(UPPER(REPLACE(REPLACE(STRING(`rowguid`), '{', ''), '}', ''))), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL'))) AS `FlexRowHash`
    FROM    '${location_path}/AdventureWorksLT2012_SalesLT_CustomerAddress'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_CustomerAddress_DELTA`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_CustomerAddress_DELTA` AS
SELECT	 STG.`CustomerAddress_BK`
		,STG.`CustomerID`
		,STG.`AddressID`
		,STG.`AddressType`
		,STG.`rowguid`
		,STG.`ModifiedDate`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowChangeType`
		,STG.`FlexRowRecordSource`
		,STG.`FlexRowHash`
FROM
(
	SELECT	 SRC.`CustomerAddress_BK`
			,SRC.`CustomerID`
			,SRC.`AddressID`
			,SRC.`AddressType`
			,SRC.`rowguid`
			,SRC.`ModifiedDate`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowChangeType`
			,SRC.`FlexRowRecordSource`
			,SRC.`FlexRowHash`
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`CustomerID`,SRC.`AddressID`,SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`CustomerID`,SRC.`AddressID`, SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`CustomerID`,SRC.`AddressID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`CustomerID`,SRC.`AddressID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_CustomerAddress` AS SRC
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`CustomerAddress`;
INSERT INTO `${_bfx_stg}`.`awlt`.`CustomerAddress`
        (`CustomerAddress_BK`
		,`CustomerID`
		,`AddressID`
		,`AddressType`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT   SRC.`CustomerAddress_BK`
		,SRC.`CustomerID`
		,SRC.`AddressID`
		,SRC.`AddressType`
		,SRC.`rowguid`
		,SRC.`ModifiedDate`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,SRC.`FlexRowChangeType`
		,SRC.`FlexRowRecordSource`
		,SRC.`FlexRowHash`
FROM	`TMP_AdventureWorksLT2012_SalesLT_CustomerAddress_DELTA` SRC;

-- COMMAND ----------
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
SELECT   INS.`CustomerID`
		,INS.`AddressID`
		,INS.`AddressType`
		,INS.`rowguid`
		,INS.`ModifiedDate`
		,INS.`FlexRowEffectiveFromDate`
		,INS.`FlexRowAuditId`
		,INS.`FlexRowChangeType`
		,INS.`FlexRowRecordSource`
		,INS.`FlexRowHash`
FROM	`${_bfx_stg}`.`awlt`.`CustomerAddress` INS;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_CustomerAddress`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_CustomerAddress_DELTA`;

