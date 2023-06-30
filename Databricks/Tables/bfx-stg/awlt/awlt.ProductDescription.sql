-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT "bfx-stg";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-stg@lakehouse.dfs.core.windows.net";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `${catalog_name}`.`awlt`.`ProductDescription`(
	`ProductDescriptionID`          INT	NOT NULL,
	`Description`                   STRING,
	`rowguid`                       STRING,
	`ModifiedDate`                  TIMESTAMP,
	`FlexRowEffectiveFromDate`      TIMESTAMP,
	`FlexRowAuditId`                BIGINT,
	`FlexRowChangeType`             STRING,
	`FlexRowRecordSource`           STRING,
	`FlexRowHash`                   BINARY)
USING DELTA
	LOCATION '${location_path}/awlt/ProductDescription'
	TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
