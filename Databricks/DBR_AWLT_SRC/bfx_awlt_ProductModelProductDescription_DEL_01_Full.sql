-- Databricks notebook source
-- Delete Detection notebook bfx_awlt_ProductModelProductDescription_DEL
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";



--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductModelPro_001_DEL_LND`;

CREATE TABLE IF NOT EXISTS `TMP_AWLT_02_Sales_01_ProductModelPro_001_DEL_LND`;
COPY INTO `TMP_AWLT_02_Sales_01_ProductModelPro_001_DEL_LND`
FROM
(
    SELECT   CAST('awlt'||'~'||UPPER(COALESCE(TRIM(STRING(`ProductModelID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(`ProductDescriptionID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(`Culture`)),'NVL')) AS STRING) AS `ProductModelProductDescription_BK`
			,CAST(`ProductModelID` AS INT) AS `ProductModelID`
			,CAST(`ProductDescriptionID` AS INT) AS `ProductDescriptionID`
			,CAST(`Culture` AS STRING) AS `Culture`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/AdventureWorksLT2012_awlt_ProductModelProductDescription_DEL'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

--COMMAND----------

DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductModelPro_001_DEL_CACHE`;

CREATE TABLE `TMP_AWLT_02_Sales_01_ProductModelPro_001_DEL_CACHE` AS
SELECT	 CAST('awlt'||'~'||UPPER(COALESCE(TRIM(STRING(PSA.`ProductModelID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(PSA.`ProductDescriptionID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(PSA.`Culture`)),'NVL')) AS STRING) AS `ProductModelProductDescription_BK`
		,PSA.`ProductModelID` AS `ProductModelID`
		,PSA.`ProductDescriptionID` AS `ProductDescriptionID`
		,CAST(NULLIF(LTRIM(RTRIM(PSA.`Culture`)), '') AS STRING) AS `Culture`
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}`.`awlt`.`ProductModelProductDescription` PSA
INNER JOIN
(	SELECT	 STG.`ProductModelID`
			,STG.`ProductDescriptionID`
			,STG.`Culture`
			,MAX(STG.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`${_bfx_ods}`.`awlt`.`ProductModelProductDescription` STG
	GROUP BY STG.`ProductModelID`
			,STG.`ProductDescriptionID`
			,STG.`Culture`
) GRP
	ON	PSA.`ProductModelID` = GRP.`ProductModelID`
	AND	PSA.`ProductDescriptionID` = GRP.`ProductDescriptionID`
	AND	PSA.`Culture` = GRP.`Culture`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D';
--COMMAND----------

TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`ProductModelProductDescription_DEL`;

INSERT INTO `${_bfx_stg}`.`awlt`.`ProductModelProductDescription_DEL`
		(`ProductModelProductDescription_BK`
		,`ProductModelID`
		,`ProductDescriptionID`
		,`Culture`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT	 CAST(PSA.`ProductModelProductDescription_BK` AS STRING) AS `ProductModelProductDescription_BK`
		,CAST(PSA.`ProductModelID` AS INT) AS `ProductModelID`
		,CAST(PSA.`ProductDescriptionID` AS INT) AS `ProductDescriptionID`
		,CAST(PSA.`Culture` AS STRING) AS `Culture`
		,FLX.`FlexRowEffectiveFromDate`
		,FLX.`FlexRowAuditId`
		,FLX.`FlexRowRecordSource`
FROM	`TMP_AWLT_02_Sales_01_ProductModelPro_001_DEL_CACHE` PSA
LEFT OUTER JOIN `TMP_AWLT_02_Sales_01_ProductModelPro_001_DEL_LND` LND
	ON	PSA.`ProductModelProductDescription_BK` = LND.`ProductModelProductDescription_BK`
	AND	PSA.`Culture` = LND.`Culture`
CROSS JOIN
(	SELECT	 TOP 1 `FlexRowEffectiveFromDate`
			,`FlexRowAuditId`
			,`FlexRowRecordSource`
	FROM	`TMP_AWLT_02_Sales_01_ProductModelPro_001_DEL_LND`
) AS FLX
WHERE	LND.`ProductModelProductDescription_BK` IS NULL

--COMMAND----------

INSERT INTO `${_bfx_ods}`.`awlt`.`ProductModelProductDescription`
		(`ProductModelID`
		,`ProductDescriptionID`
		,`Culture`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT	 PSA.`ProductModelID`
		,PSA.`ProductDescriptionID`
		,PSA.`Culture`
		,PSA.`rowguid`
		,PSA.`ModifiedDate`
		,DEL.`FlexRowEffectiveFromDate`
		,DEL.`FlexRowAuditId`
		,CAST('D' AS CHAR(1)) AS `FlexRowChangeType`
		,DEL.`FlexRowRecordSource`
		,PSA.`FlexRowHash`
FROM	`${_bfx_ods}`.`awlt`.`ProductModelProductDescription` PSA
INNER JOIN `TMP_AWLT_02_Sales_01_ProductModelPro_001_DEL_CACHE` GRP
	ON	PSA.`ProductModelID` = GRP.`ProductModelID`
	AND	PSA.`ProductDescriptionID` = GRP.`ProductDescriptionID`
	AND	PSA.`Culture` = GRP.`Culture`
	AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
	AND PSA.`FlexRowChangeType` <> 'D'
INNER JOIN `${_bfx_stg}`.`awlt`.`ProductModelProductDescription_DEL` DEL
	ON	DEL.`ProductModelID` = PSA.`ProductModelID`
	AND	DEL.`ProductDescriptionID` = PSA.`ProductDescriptionID`
	AND	DEL.`Culture` = PSA.`Culture`;

--COMMAND----------
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductModelPro_001_DEL_LND`;
DROP TABLE IF EXISTS `TMP_AWLT_02_Sales_01_ProductModelPro_001_DEL_CACHE`;