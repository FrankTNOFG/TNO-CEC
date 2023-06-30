-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT "bfx-stg";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-stg@lakehouse.dfs.core.windows.net";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `${catalog_name}`.`awff`.`Account`(
	`Account_BK`                         STRING	NOT NULL,
	`AccountCodeAlternateKey`            INT	NOT NULL,
	`ParentAccountCodeAlternateKey`      INT,
	`AccountDescription`                 STRING,
	`AccountType`                        STRING,
	`Operator`                           STRING,
	`CustomMembers`                      STRING,
	`ValueType`                          STRING,
	`CustomMemberOptions`                STRING,
	`FlexRowEffectiveFromDate`           TIMESTAMP,
	`FlexRowAuditId`                     BIGINT,
	`FlexRowRecordSource`                STRING)
USING DELTA
	LOCATION '${location_path}/awff/Account'
	TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
