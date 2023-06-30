-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT "bfx-ods";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-ods@lakehouse.dfs.core.windows.net";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `${catalog_name}`.`awlt`.`Customer`(
	`CustomerID`                    INT	NOT NULL,
	`NameStyle`                     BOOLEAN,
	`Title`                         STRING,
	`FirstName`                     STRING,
	`MiddleName`                    STRING,
	`LastName`                      STRING,
	`Suffix`                        STRING,
	`CompanyName`                   STRING,
	`SalesPerson`                   STRING,
	`EmailAddress`                  STRING,
	`Phone`                         STRING,
	`PasswordHash`                  STRING,
	`PasswordSalt`                  STRING,
	`rowguid`                       STRING,
	`ModifiedDate`                  TIMESTAMP,
	`FlexRowEffectiveFromDate`      TIMESTAMP,
	`FlexRowAuditId`                BIGINT,
	`FlexRowChangeType`             STRING,
	`FlexRowRecordSource`           STRING,
	`FlexRowHash`                   BINARY)
USING DELTA
	LOCATION '${location_path}/awlt/Customer'
	TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
