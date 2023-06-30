-- Databricks notebook source
-- Full load notebook bfx_awlt_Customer
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_Customer`;
CREATE TABLE IF NOT EXISTS `TMP_AdventureWorksLT2012_SalesLT_Customer`;

COPY INTO `TMP_AdventureWorksLT2012_SalesLT_Customer`
FROM
(
    SELECT   CAST(`CustomerID` AS INT) AS `CustomerID`
			,CAST(`NameStyle` AS BOOLEAN) AS `NameStyle`
			,CAST(`Title` AS STRING) AS `Title`
			,CAST(`FirstName` AS STRING) AS `FirstName`
			,CAST(`MiddleName` AS STRING) AS `MiddleName`
			,CAST(`LastName` AS STRING) AS `LastName`
			,CAST(`Suffix` AS STRING) AS `Suffix`
			,CAST(`CompanyName` AS STRING) AS `CompanyName`
			,CAST(`SalesPerson` AS STRING) AS `SalesPerson`
			,CAST(`EmailAddress` AS STRING) AS `EmailAddress`
			,CAST(`Phone` AS STRING) AS `Phone`
			,CAST(`PasswordHash` AS STRING) AS `PasswordHash`
			,CAST(`PasswordSalt` AS STRING) AS `PasswordSalt`
			,CAST(`rowguid` AS STRING) AS `rowguid`
			,TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')) AS `ModifiedDate`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowChangeType` AS STRING) AS `FlexRowChangeType`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
			,SHA1(STRING(
				COALESCE(TRIM(CAST(`NameStyle` AS STRING)), 'NVL')||'~'||
				COALESCE(TRIM(`Title`), 'NVL')||'~'||
				COALESCE(TRIM(`FirstName`), 'NVL')||'~'||
				COALESCE(TRIM(`MiddleName`), 'NVL')||'~'||
				COALESCE(TRIM(`LastName`), 'NVL')||'~'||
				COALESCE(TRIM(`Suffix`), 'NVL')||'~'||
				COALESCE(TRIM(`CompanyName`), 'NVL')||'~'||
				COALESCE(TRIM(`SalesPerson`), 'NVL')||'~'||
				COALESCE(TRIM(`EmailAddress`), 'NVL')||'~'||
				COALESCE(TRIM(`Phone`), 'NVL')||'~'||
				COALESCE(TRIM(`PasswordHash`), 'NVL')||'~'||
				COALESCE(TRIM(`PasswordSalt`), 'NVL')||'~'||
				COALESCE(TRIM(UPPER(REPLACE(REPLACE(STRING(`rowguid`), '{', ''), '}', ''))), 'NVL')||'~'||
				COALESCE(TRIM(DATE_FORMAT(TO_TIMESTAMP(DATE_FORMAT(`ModifiedDate`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')), 'NVL'))) AS `FlexRowHash`
    FROM    '${location_path}/AdventureWorksLT2012_SalesLT_Customer'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awlt`.`Customer`;
INSERT INTO `${_bfx_stg}`.`awlt`.`Customer`
        (`CustomerID`
		,`NameStyle`
		,`Title`
		,`FirstName`
		,`MiddleName`
		,`LastName`
		,`Suffix`
		,`CompanyName`
		,`SalesPerson`
		,`EmailAddress`
		,`Phone`
		,`PasswordHash`
		,`PasswordSalt`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT   SRC.`CustomerID`
		,SRC.`NameStyle`
		,SRC.`Title`
		,SRC.`FirstName`
		,SRC.`MiddleName`
		,SRC.`LastName`
		,SRC.`Suffix`
		,SRC.`CompanyName`
		,SRC.`SalesPerson`
		,SRC.`EmailAddress`
		,SRC.`Phone`
		,SRC.`PasswordHash`
		,SRC.`PasswordSalt`
		,SRC.`rowguid`
		,SRC.`ModifiedDate`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,SRC.`FlexRowChangeType`
		,SRC.`FlexRowRecordSource`
		,SRC.`FlexRowHash`
FROM	`TMP_AdventureWorksLT2012_SalesLT_Customer` SRC;

-- COMMAND ----------
INSERT INTO `${_bfx_ods}`.`awlt`.`Customer`
        (`CustomerID`
		,`NameStyle`
		,`Title`
		,`FirstName`
		,`MiddleName`
		,`LastName`
		,`Suffix`
		,`CompanyName`
		,`SalesPerson`
		,`EmailAddress`
		,`Phone`
		,`PasswordHash`
		,`PasswordSalt`
		,`rowguid`
		,`ModifiedDate`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowChangeType`
		,`FlexRowRecordSource`
		,`FlexRowHash`)
SELECT   INS.`CustomerID`
		,INS.`NameStyle`
		,INS.`Title`
		,INS.`FirstName`
		,INS.`MiddleName`
		,INS.`LastName`
		,INS.`Suffix`
		,INS.`CompanyName`
		,INS.`SalesPerson`
		,INS.`EmailAddress`
		,INS.`Phone`
		,INS.`PasswordHash`
		,INS.`PasswordSalt`
		,INS.`rowguid`
		,INS.`ModifiedDate`
		,INS.`FlexRowEffectiveFromDate`
		,INS.`FlexRowAuditId`
		,INS.`FlexRowChangeType`
		,INS.`FlexRowRecordSource`
		,INS.`FlexRowHash`
FROM	`${_bfx_stg}`.`awlt`.`Customer` INS;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_AdventureWorksLT2012_SalesLT_Customer`;
