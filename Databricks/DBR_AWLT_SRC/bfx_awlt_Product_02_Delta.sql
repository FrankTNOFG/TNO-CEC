-- Databricks notebook source
-- Full load notebook bfx_awlt_Product
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


TRUNCATE TABLE `${_bfx_ods}`.`awlt`.`Product`; 

-- COMMAND ----------
INSERT INTO `${_bfx_ods}`.`awlt`.`Product`
        (`ProductID`
		,`Name`
		,`ProductNumber`
		,`Color`
		,`StandardCost`
		,`ListPrice`
		,`Size`
		,`Weight`
		,`ProductCategoryID`
		,`ProductModelID`
		,`SellStartDate`
		,`SellEndDate`
		,`DiscontinuedDate`
		,`ThumbnailPhotoFileName`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
COPY INTO `TMP_AdventureWorksLT2012_SalesLT_Product`
FROM
(
    SELECT   CAST(`ProductID` AS INT) AS `ProductID`
			,CAST(`Name` AS STRING) AS `Name`
			,CAST(`ProductNumber` AS STRING) AS `ProductNumber`
			,CAST(`Color` AS STRING) AS `Color`
			,CAST(`StandardCost` AS DECIMAL(19,4)) AS `StandardCost`
			,CAST(`ListPrice` AS DECIMAL(19,4)) AS `ListPrice`
			,CAST(`Size` AS STRING) AS `Size`
			,CAST(`Weight` AS DECIMAL(8,2)) AS `Weight`
			,CAST(`ProductCategoryID` AS INT) AS `ProductCategoryID`
			,CAST(`ProductModelID` AS INT) AS `ProductModelID`
			,TO_TIMESTAMP(DATE_FORMAT(`SellStartDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `SellStartDate`
			,TO_TIMESTAMP(DATE_FORMAT(`SellEndDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `SellEndDate`
			,TO_TIMESTAMP(DATE_FORMAT(`DiscontinuedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `DiscontinuedDate`
			,CAST(`ThumbnailPhotoFileName` AS STRING) AS `ThumbnailPhotoFileName`
			,CAST(`rowguid` AS STRING) AS `rowguid`
			,TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `ModifiedDate`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
			,SHA1(STRING(
				COALESCE(TRIM(`Name`), 'NVL')||'~'||
				COALESCE(TRIM(`ProductNumber`), 'NVL')||'~'||
				COALESCE(TRIM(`Color`), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`StandardCost` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`ListPrice` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(`Size`), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`Weight` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`ProductCategoryID` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(CAST(`ProductModelID` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`SellStartDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`SellEndDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`DiscontinuedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL')||'~'||
				COALESCE(TRIM(`ThumbnailPhotoFileName`), 'NVL')||'~'||
				COALESCE(TRIM(UPPER(REPLACE(REPLACE(STRING(`rowguid`), '{', ''), '}', ''))), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL'))) AS `FlexRowHash`
    FROM    '${location_path}/AdventureWorksLT2012_SalesLT_Product'
);
