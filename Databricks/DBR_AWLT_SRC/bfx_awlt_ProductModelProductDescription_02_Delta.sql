-- Databricks notebook source
-- Delta load notebook bfx_awlt_ProductModelProductDescription
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription`;
CREATE TABLE IF NOT EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription`;

COPY INTO `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription`
FROM
(
    SELECT   CAST('awlt'||'~'||UPPER(COALESCE(TRIM(STRING(`ProductModelID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(`ProductDescriptionID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(`Culture`)),'NVL')) AS STRING) AS `ProductModelProductDescription_BK`
			,CAST(`ProductModelID` AS INT) AS `ProductModelID`
			,CAST(`ProductDescriptionID` AS INT) AS `ProductDescriptionID`
			,CAST(`Culture` AS STRING) AS `Culture`
			,CAST(`rowguid` AS STRING) AS `rowguid`
			,TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `ModifiedDate`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
			,SHA1(STRING(
				COALESCE(TRIM(UPPER(REPLACE(REPLACE(STRING(`rowguid`), '{', ''), '}', ''))), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL'))) AS `FlexRowHash`
    FROM    '${location_path}/AdventureWorksLT2012_SalesLT_ProductModelProductDescription'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_CURRENT`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_CURRENT` AS 
SELECT	 PSA.`ProductModelID`
		,PSA.`ProductDescriptionID`
		,PSA.`Culture`
		,PSA.`FlexRowHash`
		,PSA.`FlexRowEffectiveFromDate`
		,PSA.`FlexRowChangeType`
FROM	`${_bfx_ods}``awlt`.`ProductModelProductDescription` PSA
INNER JOIN
(
    SELECT	 CUR.`ProductModelID`
			,CUR.`ProductDescriptionID`
			,CUR.`Culture`
            ,MAX(CUR.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
    FROM	`${_bfx_ods}`.`awlt`.`ProductModelProductDescription` CUR
    INNER JOIN `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription` TMP
        ON  CUR.`ProductModelID` = TMP.`ProductModelID`
		AND	CUR.`ProductDescriptionID` = TMP.`ProductDescriptionID`
		AND	CUR.`Culture` = TMP.`Culture`
    GROUP BY CUR.`ProductModelID`
			,CUR.`ProductDescriptionID`
			,CUR.`Culture`
) GRP
    ON  PSA.`ProductModelID` = GRP.`ProductModelID`
	AND	PSA.`ProductDescriptionID` = GRP.`ProductDescriptionID`
	AND	PSA.`Culture` = GRP.`Culture`
    AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
WHERE	PSA.`FlexRowChangeType` <> 'D';

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_DELTA`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_DELTA` AS
SELECT	 STG.`ProductModelProductDescription_BK`
		,STG.`ProductModelID`
		,STG.`ProductDescriptionID`
		,STG.`Culture`
		,STG.`rowguid`
		,STG.`ModifiedDate`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowChangeType`
		,STG.`FlexRowRecordSource`
		,STG.`FlexRowHash`
FROM
(
	SELECT	 SRC.`ProductModelProductDescription_BK`
			,SRC.`ProductModelID`
			,SRC.`ProductDescriptionID`
			,SRC.`Culture`
			,SRC.`rowguid`
			,SRC.`ModifiedDate`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowChangeType`
			,SRC.`FlexRowRecordSource`
			,SRC.`FlexRowHash`
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`ProductModelID`,SRC.`ProductDescriptionID`,SRC.`Culture`,SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`ProductModelID`,SRC.`ProductDescriptionID`,SRC.`Culture`, SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`ProductModelID`,SRC.`ProductDescriptionID`,SRC.`Culture` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`ProductModelID`,SRC.`ProductDescriptionID`,SRC.`Culture` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription` AS SRC
	LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_CURRENT` AS TGT
		ON	SRC.`ProductModelID` = TGT.`ProductModelID`
		AND	SRC.`ProductDescriptionID` = TGT.`ProductDescriptionID`
		AND	SRC.`Culture` = TGT.`Culture`
		AND SRC.`FlexRowEffectiveFromDate` <= TGT.`FlexRowEffectiveFromDate`
    WHERE	TGT.`ProductModelID` IS NULL
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_EXISTS`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_EXISTS` AS 
SELECT	 STG.`ProductModelID`
		,STG.`ProductDescriptionID`
		,STG.`Culture`
		,STG.`FlexRowHash`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowChangeType`
FROM	`TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_DELTA` STG
INNER JOIN
(
	SELECT	 SRC.`ProductModelID`
			,SRC.`ProductDescriptionID`
			,SRC.`Culture`
			,MIN(SRC.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_DELTA` SRC
	GROUP BY SRC.`ProductModelID`
			,SRC.`ProductDescriptionID`
			,SRC.`Culture`
) GRP
	ON	STG.`ProductModelID` = GRP.`ProductModelID`
	AND	STG.`ProductDescriptionID` = GRP.`ProductDescriptionID`
	AND	STG.`Culture` = GRP.`Culture`
	AND STG.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
INNER JOIN `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_CURRENT` CUR
    ON	STG.`ProductModelID` = CUR.`ProductModelID`
	AND	STG.`ProductDescriptionID` = CUR.`ProductDescriptionID`
	AND	STG.`Culture` = CUR.`Culture`
    AND STG.`FlexRowHash` = CUR.`FlexRowHash`;


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`ProductModelProductDescription`;
INSERT INTO `${_bfx_stg}`.`awlt`.`ProductModelProductDescription`
        (`ProductModelProductDescription_BK`
		,`ProductModelID`
		,`ProductDescriptionID`
		,`Culture`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT   SRC.`ProductModelProductDescription_BK`
		,SRC.`ProductModelID`
		,SRC.`ProductDescriptionID`
		,SRC.`Culture`
		,SRC.`rowguid`
		,SRC.`ModifiedDate`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,CASE WHEN TGT.`FlexRowChangeType` IS NULL THEN 'I' ELSE 'U' END
		,SRC.`FlexRowRecordSource`
		,SRC.`FlexRowHash`
FROM	`TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_DELTA` SRC
LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_CURRENT` TGT
    ON  SRC.`ProductModelID` = TGT.`ProductModelID`
	AND	SRC.`ProductDescriptionID` = TGT.`ProductDescriptionID`
	AND	SRC.`Culture` = TGT.`Culture`
LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_EXISTS` PSA
    ON  SRC.`ProductModelID` = PSA.`ProductModelID`
	AND	SRC.`ProductDescriptionID` = PSA.`ProductDescriptionID`
	AND	SRC.`Culture` = PSA.`Culture`
    AND SRC.`FlexRowEffectiveFromDate` = PSA.`FlexRowEffectiveFromDate`
WHERE   PSA.`ProductModelID` IS NULL;

-- COMMAND ----------
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
SELECT   INS.`ProductModelID`
		,INS.`ProductDescriptionID`
		,INS.`Culture`
		,INS.`rowguid`
		,INS.`ModifiedDate`
		,INS.`FlexRowEffectiveFromDate`
		,INS.`FlexRowAuditId`
		,INS.`FlexRowChangeType`
		,INS.`FlexRowRecordSource`
		,INS.`FlexRowHash`
FROM	`${_bfx_stg}`.`awlt`.`ProductModelProductDescription` INS;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_CURRENT`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_DELTA`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_ProductModelProductDescription_EXISTS`;

