-- Databricks notebook source
-- Delta load notebook bfx_awlt_SalesOrderDetail
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail`;
CREATE TABLE IF NOT EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail`;

COPY INTO `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail`
FROM
(
    SELECT   CAST('awlt'||'~'||UPPER(COALESCE(TRIM(STRING(`SalesOrderID`)),'NVL'))||'~'||UPPER(COALESCE(TRIM(STRING(`SalesOrderDetailID`)),'NVL')) AS STRING) AS `SalesOrderDetail_BK`
			,CAST(`SalesOrderID` AS INT) AS `SalesOrderID`
			,CAST(`SalesOrderDetailID` AS INT) AS `SalesOrderDetailID`
			,CAST(`OrderQty` AS SMALLINT) AS `OrderQty`
			,CAST(`ProductID` AS INT) AS `ProductID`
			,CAST(`UnitPrice` AS DECIMAL(19,4)) AS `UnitPrice`
			,CAST(`UnitPriceDiscount` AS DECIMAL(19,4)) AS `UnitPriceDiscount`
			,CAST(`LineTotal` AS DECIMAL(38,6)) AS `LineTotal`
			,CAST(`rowguid` AS STRING) AS `rowguid`
			,TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `ModifiedDate`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
			,SHA1(STRING(
				COALESCE(TRIM(CAST(`OrderQty` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`ProductID` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`UnitPrice` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`UnitPriceDiscount` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`LineTotal` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(UPPER(REPLACE(REPLACE(STRING(`rowguid`), '{', ''), '}', ''))), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL'))) AS `FlexRowHash`
    FROM    '${location_path}/AdventureWorksLT2012_SalesLT_SalesOrderDetail'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_CURRENT`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_CURRENT` AS 
SELECT	 PSA.`SalesOrderID`
		,PSA.`SalesOrderDetailID`
		,PSA.`FlexRowHash`
		,PSA.`FlexRowEffectiveFromDate`
		,PSA.`FlexRowChangeType`
FROM	`${_bfx_ods}``awlt`.`SalesOrderDetail` PSA
INNER JOIN
(
    SELECT	 CUR.`SalesOrderID`
			,CUR.`SalesOrderDetailID`
            ,MAX(CUR.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
    FROM	`${_bfx_ods}`.`awlt`.`SalesOrderDetail` CUR
    INNER JOIN `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail` TMP
        ON  CUR.`SalesOrderID` = TMP.`SalesOrderID`
		AND	CUR.`SalesOrderDetailID` = TMP.`SalesOrderDetailID`
    GROUP BY CUR.`SalesOrderID`
			,CUR.`SalesOrderDetailID`
) GRP
    ON  PSA.`SalesOrderID` = GRP.`SalesOrderID`
	AND	PSA.`SalesOrderDetailID` = GRP.`SalesOrderDetailID`
    AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
WHERE	PSA.`FlexRowChangeType` <> 'D';

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_DELTA`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_DELTA` AS
SELECT	 STG.`SalesOrderDetail_BK`
		,STG.`SalesOrderID`
		,STG.`SalesOrderDetailID`
		,STG.`OrderQty`
		,STG.`ProductID`
		,STG.`UnitPrice`
		,STG.`UnitPriceDiscount`
		,STG.`LineTotal`
		,STG.`rowguid`
		,STG.`ModifiedDate`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowChangeType`
		,STG.`FlexRowRecordSource`
		,STG.`FlexRowHash`
FROM
(
	SELECT	 SRC.`SalesOrderDetail_BK`
			,SRC.`SalesOrderID`
			,SRC.`SalesOrderDetailID`
			,SRC.`OrderQty`
			,SRC.`ProductID`
			,SRC.`UnitPrice`
			,SRC.`UnitPriceDiscount`
			,SRC.`LineTotal`
			,SRC.`rowguid`
			,SRC.`ModifiedDate`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowChangeType`
			,SRC.`FlexRowRecordSource`
			,SRC.`FlexRowHash`
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`SalesOrderID`,SRC.`SalesOrderDetailID`,SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`SalesOrderID`,SRC.`SalesOrderDetailID`, SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`SalesOrderID`,SRC.`SalesOrderDetailID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`SalesOrderID`,SRC.`SalesOrderDetailID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail` AS SRC
	LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_CURRENT` AS TGT
		ON	SRC.`SalesOrderID` = TGT.`SalesOrderID`
		AND	SRC.`SalesOrderDetailID` = TGT.`SalesOrderDetailID`
		AND SRC.`FlexRowEffectiveFromDate` <= TGT.`FlexRowEffectiveFromDate`
    WHERE	TGT.`SalesOrderID` IS NULL
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_EXISTS`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_EXISTS` AS 
SELECT	 STG.`SalesOrderID`
		,STG.`SalesOrderDetailID`
		,STG.`FlexRowHash`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowChangeType`
FROM	`TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_DELTA` STG
INNER JOIN
(
	SELECT	 SRC.`SalesOrderID`
			,SRC.`SalesOrderDetailID`
			,MIN(SRC.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_DELTA` SRC
	GROUP BY SRC.`SalesOrderID`
			,SRC.`SalesOrderDetailID`
) GRP
	ON	STG.`SalesOrderID` = GRP.`SalesOrderID`
	AND	STG.`SalesOrderDetailID` = GRP.`SalesOrderDetailID`
	AND STG.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
INNER JOIN `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_CURRENT` CUR
    ON	STG.`SalesOrderID` = CUR.`SalesOrderID`
	AND	STG.`SalesOrderDetailID` = CUR.`SalesOrderDetailID`
    AND STG.`FlexRowHash` = CUR.`FlexRowHash`;


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`SalesOrderDetail`;
INSERT INTO `${_bfx_stg}`.`awlt`.`SalesOrderDetail`
        (`SalesOrderDetail_BK`
		,`SalesOrderID`
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
SELECT   SRC.`SalesOrderDetail_BK`
		,SRC.`SalesOrderID`
		,SRC.`SalesOrderDetailID`
		,SRC.`OrderQty`
		,SRC.`ProductID`
		,SRC.`UnitPrice`
		,SRC.`UnitPriceDiscount`
		,SRC.`LineTotal`
		,SRC.`rowguid`
		,SRC.`ModifiedDate`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,CASE WHEN TGT.`FlexRowChangeType` IS NULL THEN 'I' ELSE 'U' END
		,SRC.`FlexRowRecordSource`
		,SRC.`FlexRowHash`
FROM	`TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_DELTA` SRC
LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_CURRENT` TGT
    ON  SRC.`SalesOrderID` = TGT.`SalesOrderID`
	AND	SRC.`SalesOrderDetailID` = TGT.`SalesOrderDetailID`
LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_EXISTS` PSA
    ON  SRC.`SalesOrderID` = PSA.`SalesOrderID`
	AND	SRC.`SalesOrderDetailID` = PSA.`SalesOrderDetailID`
    AND SRC.`FlexRowEffectiveFromDate` = PSA.`FlexRowEffectiveFromDate`
WHERE   PSA.`SalesOrderID` IS NULL;

-- COMMAND ----------
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
SELECT   INS.`SalesOrderID`
		,INS.`SalesOrderDetailID`
		,INS.`OrderQty`
		,INS.`ProductID`
		,INS.`UnitPrice`
		,INS.`UnitPriceDiscount`
		,INS.`LineTotal`
		,INS.`rowguid`
		,INS.`ModifiedDate`
		,INS.`FlexRowEffectiveFromDate`
		,INS.`FlexRowAuditId`
		,INS.`FlexRowChangeType`
		,INS.`FlexRowRecordSource`
		,INS.`FlexRowHash`
FROM	`${_bfx_stg}`.`awlt`.`SalesOrderDetail` INS;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_CURRENT`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_DELTA`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderDetail_EXISTS`;

