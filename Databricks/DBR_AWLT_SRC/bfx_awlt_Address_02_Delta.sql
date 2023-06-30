-- Databricks notebook source
-- Delta load notebook bfx_awlt_Address
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_Address`;
CREATE TABLE IF NOT EXISTS `TMP_AdventureWorksLT2012_SalesLT_Address`;

COPY INTO `TMP_AdventureWorksLT2012_SalesLT_Address`
FROM
(
    SELECT   CAST(`AddressID` AS INT) AS `AddressID`
			,CAST(`AddressLine1` AS STRING) AS `AddressLine1`
			,CAST(`AddressLine2` AS STRING) AS `AddressLine2`
			,CAST(`City` AS STRING) AS `City`
			,CAST(`StateProvince` AS STRING) AS `StateProvince`
			,CAST(`CountryRegion` AS STRING) AS `CountryRegion`
			,CAST(`PostalCode` AS STRING) AS `PostalCode`
			,CAST(`rowguid` AS STRING) AS `rowguid`
			,TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `ModifiedDate`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
			,SHA1(STRING(
				COALESCE(TRIM(`AddressLine1`), 'NVL')||'~'||
				COALESCE(TRIM(`AddressLine2`), 'NVL')||'~'||
				COALESCE(TRIM(`City`), 'NVL')||'~'||
				COALESCE(TRIM(`StateProvince`), 'NVL')||'~'||
				COALESCE(TRIM(`CountryRegion`), 'NVL')||'~'||
				COALESCE(TRIM(`PostalCode`), 'NVL')||'~'||
				COALESCE(TRIM(UPPER(REPLACE(REPLACE(STRING(`rowguid`), '{', ''), '}', ''))), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL'))) AS `FlexRowHash`
    FROM    '${location_path}/AdventureWorksLT2012_SalesLT_Address'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_Address_CURRENT`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_Address_CURRENT` AS 
SELECT	 PSA.`AddressID`
		,PSA.`FlexRowHash`
		,PSA.`FlexRowEffectiveFromDate`
		,PSA.`FlexRowChangeType`
FROM	`${_bfx_ods}``awlt`.`Address` PSA
INNER JOIN
(
    SELECT	 CUR.`AddressID`
            ,MAX(CUR.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
    FROM	`${_bfx_ods}`.`awlt`.`Address` CUR
    INNER JOIN `TMP_AdventureWorksLT2012_SalesLT_Address` TMP
        ON  CUR.`AddressID` = TMP.`AddressID`
    GROUP BY CUR.`AddressID`
) GRP
    ON  PSA.`AddressID` = GRP.`AddressID`
    AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
WHERE	PSA.`FlexRowChangeType` <> 'D';

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_Address_DELTA`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_Address_DELTA` AS
SELECT	 STG.`AddressID`
		,STG.`AddressLine1`
		,STG.`AddressLine2`
		,STG.`City`
		,STG.`StateProvince`
		,STG.`CountryRegion`
		,STG.`PostalCode`
		,STG.`rowguid`
		,STG.`ModifiedDate`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowChangeType`
		,STG.`FlexRowRecordSource`
		,STG.`FlexRowHash`
FROM
(
	SELECT	 SRC.`AddressID`
			,SRC.`AddressLine1`
			,SRC.`AddressLine2`
			,SRC.`City`
			,SRC.`StateProvince`
			,SRC.`CountryRegion`
			,SRC.`PostalCode`
			,SRC.`rowguid`
			,SRC.`ModifiedDate`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowChangeType`
			,SRC.`FlexRowRecordSource`
			,SRC.`FlexRowHash`
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`AddressID`,SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`AddressID`, SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`AddressID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`AddressID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_Address` AS SRC
	LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_Address_CURRENT` AS TGT
		ON	SRC.`AddressID` = TGT.`AddressID`
		AND SRC.`FlexRowEffectiveFromDate` <= TGT.`FlexRowEffectiveFromDate`
    WHERE	TGT.`AddressID` IS NULL
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_Address_EXISTS`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_Address_EXISTS` AS 
SELECT	 STG.`AddressID`
		,STG.`FlexRowHash`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowChangeType`
FROM	`TMP_AdventureWorksLT2012_SalesLT_Address_DELTA` STG
INNER JOIN
(
	SELECT	 SRC.`AddressID`
			,MIN(SRC.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_Address_DELTA` SRC
	GROUP BY SRC.`AddressID`
) GRP
	ON	STG.`AddressID` = GRP.`AddressID`
	AND STG.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
INNER JOIN `TMP_AdventureWorksLT2012_SalesLT_Address_CURRENT` CUR
    ON	STG.`AddressID` = CUR.`AddressID`
    AND STG.`FlexRowHash` = CUR.`FlexRowHash`;


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`Address`;
INSERT INTO `${_bfx_stg}`.`awlt`.`Address`
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
SELECT   SRC.`AddressID`
		,SRC.`AddressLine1`
		,SRC.`AddressLine2`
		,SRC.`City`
		,SRC.`StateProvince`
		,SRC.`CountryRegion`
		,SRC.`PostalCode`
		,SRC.`rowguid`
		,SRC.`ModifiedDate`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,CASE WHEN TGT.`FlexRowChangeType` IS NULL THEN 'I' ELSE 'U' END
		,SRC.`FlexRowRecordSource`
		,SRC.`FlexRowHash`
FROM	`TMP_AdventureWorksLT2012_SalesLT_Address_DELTA` SRC
LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_Address_CURRENT` TGT
    ON  SRC.`AddressID` = TGT.`AddressID`
LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_Address_EXISTS` PSA
    ON  SRC.`AddressID` = PSA.`AddressID`
    AND SRC.`FlexRowEffectiveFromDate` = PSA.`FlexRowEffectiveFromDate`
WHERE   PSA.`AddressID` IS NULL;

-- COMMAND ----------
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
SELECT   INS.`AddressID`
		,INS.`AddressLine1`
		,INS.`AddressLine2`
		,INS.`City`
		,INS.`StateProvince`
		,INS.`CountryRegion`
		,INS.`PostalCode`
		,INS.`rowguid`
		,INS.`ModifiedDate`
		,INS.`FlexRowEffectiveFromDate`
		,INS.`FlexRowAuditId`
		,INS.`FlexRowChangeType`
		,INS.`FlexRowRecordSource`
		,INS.`FlexRowHash`
FROM	`${_bfx_stg}`.`awlt`.`Address` INS;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_Address`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_Address_CURRENT`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_Address_DELTA`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_Address_EXISTS`;

