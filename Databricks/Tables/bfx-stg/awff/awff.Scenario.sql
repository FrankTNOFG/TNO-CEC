-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT "bfx-stg";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-stg@lakehouse.dfs.core.windows.net";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `${catalog_name}`.`awff`.`Scenario`(
	`ScenarioKey`                   INT	NOT NULL,
	`ScenarioName`                  STRING,
	`ScenarioDate`                  STRING,
	`ScenarioEmpty`                 STRING,
	`FlexRowEffectiveFromDate`      TIMESTAMP,
	`FlexRowAuditId`                BIGINT,
	`FlexRowRecordSource`           STRING)
USING DELTA
	LOCATION '${location_path}/awff/Scenario'
	TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
