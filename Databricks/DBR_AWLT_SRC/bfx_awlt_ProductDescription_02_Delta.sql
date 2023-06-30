-- Databricks notebook source
-- Delta load notebook bfx_awlt_ProductDescription
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductDescription`;
CREATE TABLE IF NOT EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductDescription`;

COPY INTO `TMP_AdventureWorksLT2012_SalesLT_ProductDescription`
FROM
(
    SELECT   CAST(`ProductDescriptionID` AS INT) AS `ProductDescriptionID`
			,CAST(`Description` AS STRING) AS `Description`
			,CAST(`rowguid` AS STRING) AS `rowguid`
			,TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `ModifiedDate`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
			,SHA1(STRING(
				COALESCE(TRIM(`Description`), 'NVL')||'~'||
				COALESCE(TRIM(UPPER(REPLACE(REPLACE(STRING(`rowguid`), '{', ''), '}', ''))), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL'))) AS `FlexRowHash`
    FROM    '${location_path}/AdventureWorksLT2012_SalesLT_ProductDescription'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_CURRENT`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_CURRENT` AS 
SELECT	 PSA.`ProductDescriptionID`
		,PSA.`FlexRowHash`
		,PSA.`FlexRowEffectiveFromDate`
		,PSA.`FlexRowChangeType`
FROM	`${_bfx_ods}``awlt`.`ProductDescription` PSA
INNER JOIN
(
    SELECT	 CUR.`ProductDescriptionID`
            ,MAX(CUR.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
    FROM	`${_bfx_ods}`.`awlt`.`ProductDescription` CUR
    INNER JOIN `TMP_AdventureWorksLT2012_SalesLT_ProductDescription` TMP
        ON  CUR.`ProductDescriptionID` = TMP.`ProductDescriptionID`
    GROUP BY CUR.`ProductDescriptionID`
) GRP
    ON  PSA.`ProductDescriptionID` = GRP.`ProductDescriptionID`
    AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
WHERE	PSA.`FlexRowChangeType` <> 'D';

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_DELTA`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_DELTA` AS
SELECT	 STG.`ProductDescriptionID`
		,STG.`Description`
		,STG.`rowguid`
		,STG.`ModifiedDate`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowChangeType`
		,STG.`FlexRowRecordSource`
		,STG.`FlexRowHash`
FROM
(
	SELECT	 SRC.`ProductDescriptionID`
			,SRC.`Description`
			,SRC.`rowguid`
			,SRC.`ModifiedDate`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowChangeType`
			,SRC.`FlexRowRecordSource`
			,SRC.`FlexRowHash`
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`ProductDescriptionID`,SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`ProductDescriptionID`, SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`ProductDescriptionID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`ProductDescriptionID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_ProductDescription` AS SRC
	LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_CURRENT` AS TGT
		ON	SRC.`ProductDescriptionID` = TGT.`ProductDescriptionID`
		AND SRC.`FlexRowEffectiveFromDate` <= TGT.`FlexRowEffectiveFromDate`
    WHERE	TGT.`ProductDescriptionID` IS NULL
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_EXISTS`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_EXISTS` AS 
SELECT	 STG.`ProductDescriptionID`
		,STG.`FlexRowHash`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowChangeType`
FROM	`TMP_AdventureWorksLT2012_SalesLT_ProductDescription_DELTA` STG
INNER JOIN
(
	SELECT	 SRC.`ProductDescriptionID`
			,MIN(SRC.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_ProductDescription_DELTA` SRC
	GROUP BY SRC.`ProductDescriptionID`
) GRP
	ON	STG.`ProductDescriptionID` = GRP.`ProductDescriptionID`
	AND STG.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
INNER JOIN `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_CURRENT` CUR
    ON	STG.`ProductDescriptionID` = CUR.`ProductDescriptionID`
    AND STG.`FlexRowHash` = CUR.`FlexRowHash`;


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`ProductDescription`;
INSERT INTO `${_bfx_stg}`.`awlt`.`ProductDescription`
        (`ProductDescriptionID`
		,`Description`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT   SRC.`ProductDescriptionID`
		,SRC.`Description`
		,SRC.`rowguid`
		,SRC.`ModifiedDate`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,CASE WHEN TGT.`FlexRowChangeType` IS NULL THEN 'I' ELSE 'U' END
		,SRC.`FlexRowRecordSource`
		,SRC.`FlexRowHash`
FROM	`TMP_AdventureWorksLT2012_SalesLT_ProductDescription_DELTA` SRC
LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_CURRENT` TGT
    ON  SRC.`ProductDescriptionID` = TGT.`ProductDescriptionID`
LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_EXISTS` PSA
    ON  SRC.`ProductDescriptionID` = PSA.`ProductDescriptionID`
    AND SRC.`FlexRowEffectiveFromDate` = PSA.`FlexRowEffectiveFromDate`
WHERE   PSA.`ProductDescriptionID` IS NULL;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_DELTA`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_DELTA` AS
SELECT	 STG.`ProductDescriptionID`
		,STG.`Description`
		,STG.`rowguid`
		,STG.`ModifiedDate`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowChangeType`
		,STG.`FlexRowRecordSource`
		,STG.`FlexRowHash`
FROM
(
	SELECT	 SRC.`ProductDescriptionID`
			,SRC.`Description`
			,SRC.`rowguid`
			,SRC.`ModifiedDate`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowChangeType`
			,SRC.`FlexRowRecordSource`
			,SRC.`FlexRowHash`
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`ProductDescriptionID`,SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`ProductDescriptionID`, SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`ProductDescriptionID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`ProductDescriptionID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_ProductDescription` AS SRC
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 

-- COMMAND ----------
INSERT INTO `${_bfx_ods}`.`awlt`.`ProductDescription`
        (`ProductDescriptionID`
		,`Description`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT   INS.`ProductDescriptionID`
		,INS.`Description`
		,INS.`rowguid`
		,INS.`ModifiedDate`
		,INS.`FlexRowEffectiveFromDate`
		,INS.`FlexRowAuditId`
		,INS.`FlexRowChangeType`
		,INS.`FlexRowRecordSource`
		,INS.`FlexRowHash`
FROM	`TMP_AdventureWorksLT2012_SalesLT_ProductDescription_DELTA` INS;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductDescription`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_CURRENT`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_DELTA`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductDescription_EXISTS`;

