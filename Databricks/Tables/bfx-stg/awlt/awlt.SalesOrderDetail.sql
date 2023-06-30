-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT "bfx-stg";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-stg@lakehouse.dfs.core.windows.net";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `${catalog_name}`.`awlt`.`SalesOrderDetail`(
	`SalesOrderDetail_BK`           STRING	NOT NULL,
	`SalesOrderID`                  INT	NOT NULL,
	`SalesOrderDetailID`            INT	NOT NULL,
	`OrderQty`                      SMALLINT,
	`ProductID`                     INT,
	`UnitPrice`                     DOUBLE,
	`UnitPriceDiscount`             DOUBLE,
	`LineTotal`                     DECIMAL(38, 6),
	`rowguid`                       STRING,
	`ModifiedDate`                  TIMESTAMP,
	`FlexRowEffectiveFromDate`      TIMESTAMP,
	`FlexRowAuditId`                BIGINT,
	`FlexRowChangeType`             STRING,
	`FlexRowRecordSource`           STRING,
	`FlexRowHash`                   BINARY)
USING DELTA
	LOCATION '${location_path}/awlt/SalesOrderDetail'
	TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
