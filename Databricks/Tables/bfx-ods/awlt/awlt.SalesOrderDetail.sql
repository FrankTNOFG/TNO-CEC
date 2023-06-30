-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT "bfx-ods";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-ods@lakehouse.dfs.core.windows.net";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `${catalog_name}`.`awlt`.`SalesOrderDetail`(
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
