-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT "bfx-ods";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-ods@lakehouse.dfs.core.windows.net";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `${catalog_name}`.`awff`.`Finance`(
	`FinanceKey`                       DECIMAL(38, 0)	NOT NULL,
	`CustomerID`                       DECIMAL(38, 0),
	`Date`                             STRING,
	`OrganizationAlternateKey`         INT,
	`DepartmentGroupAlternateKey`      STRING,
	`ScenarioAlternateKey`             STRING,
	`AccountCodeAlternateKey`          STRING,
	`Amount`                           STRING,
	`FlexRowEffectiveFromDate`         TIMESTAMP,
	`FlexRowAuditId`                   BIGINT,
	`FlexRowRecordSource`              STRING)
USING DELTA
	LOCATION '${location_path}/awff/Finance'
	TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
