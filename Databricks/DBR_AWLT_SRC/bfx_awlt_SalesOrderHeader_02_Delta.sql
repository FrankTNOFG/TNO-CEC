-- Databricks notebook source
-- Delta load notebook bfx_awlt_SalesOrderHeader
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader`;
CREATE TABLE IF NOT EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader`;

COPY INTO `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader`
FROM
(
    SELECT   CAST(`SalesOrderID` AS INT) AS `SalesOrderID`
			,CAST(`RevisionNumber` AS TINYINT) AS `RevisionNumber`
			,TO_TIMESTAMP(DATE_FORMAT(`OrderDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `OrderDate`
			,TO_TIMESTAMP(DATE_FORMAT(`DueDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `DueDate`
			,TO_TIMESTAMP(DATE_FORMAT(`ShipDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `ShipDate`
			,CAST(`Status` AS TINYINT) AS `Status`
			,CAST(`OnlineOrderFlag` AS BOOLEAN) AS `OnlineOrderFlag`
			,CAST(`SalesOrderNumber` AS STRING) AS `SalesOrderNumber`
			,CAST(`PurchaseOrderNumber` AS STRING) AS `PurchaseOrderNumber`
			,CAST(`AccountNumber` AS STRING) AS `AccountNumber`
			,CAST(`CustomerID` AS INT) AS `CustomerID`
			,CAST(`ShipToAddressID` AS INT) AS `ShipToAddressID`
			,CAST(`BillToAddressID` AS INT) AS `BillToAddressID`
			,CAST(`ShipMethod` AS STRING) AS `ShipMethod`
			,CAST(`CreditCardApprovalCode` AS STRING) AS `CreditCardApprovalCode`
			,CAST(`SubTotal` AS DECIMAL(19,4)) AS `SubTotal`
			,CAST(`TaxAmt` AS DECIMAL(19,4)) AS `TaxAmt`
			,CAST(`Freight` AS DECIMAL(19,4)) AS `Freight`
			,CAST(`TotalDue` AS DECIMAL(19,4)) AS `TotalDue`
			,CAST(`Comment` AS STRING) AS `Comment`
			,CAST(`rowguid` AS STRING) AS `rowguid`
			,TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `ModifiedDate`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
			,SHA1(STRING(
				COALESCE(TRIM(CAST(`RevisionNumber` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`OrderDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`DueDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`ShipDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`Status` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`OnlineOrderFlag` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(`SalesOrderNumber`), 'NVL')||'~'||
				COALESCE(TRIM(`PurchaseOrderNumber`), 'NVL')||'~'||
				COALESCE(TRIM(`AccountNumber`), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`CustomerID` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`ShipToAddressID` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`BillToAddressID` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(`ShipMethod`), 'NVL')||'~'||
				COALESCE(TRIM(`CreditCardApprovalCode`), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`SubTotal` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`TaxAmt` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`Freight` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`TotalDue` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(`Comment`), 'NVL')||'~'||
				COALESCE(TRIM(UPPER(REPLACE(REPLACE(STRING(`rowguid`), '{', ''), '}', ''))), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL'))) AS `FlexRowHash`
    FROM    '${location_path}/AdventureWorksLT2012_SalesLT_SalesOrderHeader'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_CURRENT`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_CURRENT` AS 
SELECT	 PSA.`SalesOrderID`
		,PSA.`FlexRowHash`
		,PSA.`FlexRowEffectiveFromDate`
		,PSA.`FlexRowChangeType`
FROM	`${_bfx_ods}``awlt`.`SalesOrderHeader` PSA
INNER JOIN
(
    SELECT	 CUR.`SalesOrderID`
            ,MAX(CUR.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
    FROM	`${_bfx_ods}`.`awlt`.`SalesOrderHeader` CUR
    INNER JOIN `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader` TMP
        ON  CUR.`SalesOrderID` = TMP.`SalesOrderID`
    GROUP BY CUR.`SalesOrderID`
) GRP
    ON  PSA.`SalesOrderID` = GRP.`SalesOrderID`
    AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
WHERE	PSA.`FlexRowChangeType` <> 'D';

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_DELTA`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_DELTA` AS
SELECT	 STG.`SalesOrderID`
		,STG.`RevisionNumber`
		,STG.`OrderDate`
		,STG.`DueDate`
		,STG.`ShipDate`
		,STG.`Status`
		,STG.`OnlineOrderFlag`
		,STG.`SalesOrderNumber`
		,STG.`PurchaseOrderNumber`
		,STG.`AccountNumber`
		,STG.`CustomerID`
		,STG.`ShipToAddressID`
		,STG.`BillToAddressID`
		,STG.`ShipMethod`
		,STG.`CreditCardApprovalCode`
		,STG.`SubTotal`
		,STG.`TaxAmt`
		,STG.`Freight`
		,STG.`TotalDue`
		,STG.`Comment`
		,STG.`rowguid`
		,STG.`ModifiedDate`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowChangeType`
		,STG.`FlexRowRecordSource`
		,STG.`FlexRowHash`
FROM
(
	SELECT	 SRC.`SalesOrderID`
			,SRC.`RevisionNumber`
			,SRC.`OrderDate`
			,SRC.`DueDate`
			,SRC.`ShipDate`
			,SRC.`Status`
			,SRC.`OnlineOrderFlag`
			,SRC.`SalesOrderNumber`
			,SRC.`PurchaseOrderNumber`
			,SRC.`AccountNumber`
			,SRC.`CustomerID`
			,SRC.`ShipToAddressID`
			,SRC.`BillToAddressID`
			,SRC.`ShipMethod`
			,SRC.`CreditCardApprovalCode`
			,SRC.`SubTotal`
			,SRC.`TaxAmt`
			,SRC.`Freight`
			,SRC.`TotalDue`
			,SRC.`Comment`
			,SRC.`rowguid`
			,SRC.`ModifiedDate`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowChangeType`
			,SRC.`FlexRowRecordSource`
			,SRC.`FlexRowHash`
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`SalesOrderID`,SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`SalesOrderID`, SRC.`FlexRowHash` ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`SalesOrderID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.`FlexRowHash`) OVER (PARTITION BY SRC.`SalesOrderID` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.`FlexRowHash`) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader` AS SRC
	LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_CURRENT` AS TGT
		ON	SRC.`SalesOrderID` = TGT.`SalesOrderID`
		AND SRC.`FlexRowEffectiveFromDate` <= TGT.`FlexRowEffectiveFromDate`
    WHERE	TGT.`SalesOrderID` IS NULL
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG.`FlexRowHash` <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_EXISTS`;
CREATE TABLE `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_EXISTS` AS 
SELECT	 STG.`SalesOrderID`
		,STG.`FlexRowHash`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowChangeType`
FROM	`TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_DELTA` STG
INNER JOIN
(
	SELECT	 SRC.`SalesOrderID`
			,MIN(SRC.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_DELTA` SRC
	GROUP BY SRC.`SalesOrderID`
) GRP
	ON	STG.`SalesOrderID` = GRP.`SalesOrderID`
	AND STG.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
INNER JOIN `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_CURRENT` CUR
    ON	STG.`SalesOrderID` = CUR.`SalesOrderID`
    AND STG.`FlexRowHash` = CUR.`FlexRowHash`;


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`SalesOrderHeader`;
INSERT INTO `${_bfx_stg}`.`awlt`.`SalesOrderHeader`
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
SELECT   SRC.`SalesOrderID`
		,SRC.`RevisionNumber`
		,SRC.`OrderDate`
		,SRC.`DueDate`
		,SRC.`ShipDate`
		,SRC.`Status`
		,SRC.`OnlineOrderFlag`
		,SRC.`SalesOrderNumber`
		,SRC.`PurchaseOrderNumber`
		,SRC.`AccountNumber`
		,SRC.`CustomerID`
		,SRC.`ShipToAddressID`
		,SRC.`BillToAddressID`
		,SRC.`ShipMethod`
		,SRC.`CreditCardApprovalCode`
		,SRC.`SubTotal`
		,SRC.`TaxAmt`
		,SRC.`Freight`
		,SRC.`TotalDue`
		,SRC.`Comment`
		,SRC.`rowguid`
		,SRC.`ModifiedDate`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,CASE WHEN TGT.`FlexRowChangeType` IS NULL THEN 'I' ELSE 'U' END
		,SRC.`FlexRowRecordSource`
		,SRC.`FlexRowHash`
FROM	`TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_DELTA` SRC
LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_CURRENT` TGT
    ON  SRC.`SalesOrderID` = TGT.`SalesOrderID`
LEFT OUTER JOIN `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_EXISTS` PSA
    ON  SRC.`SalesOrderID` = PSA.`SalesOrderID`
    AND SRC.`FlexRowEffectiveFromDate` = PSA.`FlexRowEffectiveFromDate`
WHERE   PSA.`SalesOrderID` IS NULL;

-- COMMAND ----------
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
SELECT   INS.`SalesOrderID`
		,INS.`RevisionNumber`
		,INS.`OrderDate`
		,INS.`DueDate`
		,INS.`ShipDate`
		,INS.`Status`
		,INS.`OnlineOrderFlag`
		,INS.`SalesOrderNumber`
		,INS.`PurchaseOrderNumber`
		,INS.`AccountNumber`
		,INS.`CustomerID`
		,INS.`ShipToAddressID`
		,INS.`BillToAddressID`
		,INS.`ShipMethod`
		,INS.`CreditCardApprovalCode`
		,INS.`SubTotal`
		,INS.`TaxAmt`
		,INS.`Freight`
		,INS.`TotalDue`
		,INS.`Comment`
		,INS.`rowguid`
		,INS.`ModifiedDate`
		,INS.`FlexRowEffectiveFromDate`
		,INS.`FlexRowAuditId`
		,INS.`FlexRowChangeType`
		,INS.`FlexRowRecordSource`
		,INS.`FlexRowHash`
FROM	`${_bfx_stg}`.`awlt`.`SalesOrderHeader` INS;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_CURRENT`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_DELTA`;
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_SalesOrderHeader_EXISTS`;

