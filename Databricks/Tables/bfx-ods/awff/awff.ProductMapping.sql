-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT "bfx-ods";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-ods@lakehouse.dfs.core.windows.net";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `${catalog_name}`.`awff`.`ProductMapping`(
	`ProductID`                     INT	NOT NULL,
	`ProductCategoryID`             INT	NOT NULL,
	`FlexRowEffectiveFromDate`      TIMESTAMP,
	`FlexRowAuditId`                BIGINT,
	`FlexRowRecordSource`           STRING)
USING DELTA
	LOCATION '${location_path}/awff/ProductMapping'
	TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
