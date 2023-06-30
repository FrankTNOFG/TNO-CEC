-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT "bfx-stg";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-stg@lakehouse.dfs.core.windows.net";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `${catalog_name}`.`awff`.`ProductMapping`(
	`ProductMapping_BK`             STRING	NOT NULL,
	`ProductID`                     INT	NOT NULL,
	`ProductCategoryID`             INT	NOT NULL,
	`Product_BK`                    STRING,
	`ProductCategory_BK`            STRING,
	`FlexRowEffectiveFromDate`      TIMESTAMP,
	`FlexRowAuditId`                BIGINT,
	`FlexRowRecordSource`           STRING)
USING DELTA
	LOCATION '${location_path}/awff/ProductMapping'
	TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
