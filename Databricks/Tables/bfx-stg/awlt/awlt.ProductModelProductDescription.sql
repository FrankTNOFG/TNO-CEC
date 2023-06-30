-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT "bfx-stg";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-stg@lakehouse.dfs.core.windows.net";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `${catalog_name}`.`awlt`.`ProductModelProductDescription`(
	`ProductModelProductDescription_BK`      STRING	NOT NULL,
	`ProductModelID`                         INT	NOT NULL,
	`ProductDescriptionID`                   INT	NOT NULL,
	`Culture`                                STRING	NOT NULL,
	`rowguid`                                STRING,
	`ModifiedDate`                           TIMESTAMP,
	`FlexRowEffectiveFromDate`               TIMESTAMP,
	`FlexRowAuditId`                         BIGINT,
	`FlexRowChangeType`                      STRING,
	`FlexRowRecordSource`                    STRING,
	`FlexRowHash`                            BINARY)
USING DELTA
	LOCATION '${location_path}/awlt/ProductModelProductDescription'
	TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
