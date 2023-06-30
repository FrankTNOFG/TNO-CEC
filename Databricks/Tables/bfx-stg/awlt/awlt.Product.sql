-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT "bfx-stg";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-stg@lakehouse.dfs.core.windows.net";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `${catalog_name}`.`awlt`.`Product`(
	`ProductID`                     INT	NOT NULL,
	`Name`                          STRING,
	`ProductNumber`                 STRING,
	`Color`                         STRING,
	`StandardCost`                  DOUBLE,
	`ListPrice`                     DOUBLE,
	`Size`                          STRING,
	`Weight`                        DECIMAL(8, 2),
	`ProductCategoryID`             INT,
	`ProductModelID`                INT,
	`SellStartDate`                 TIMESTAMP,
	`SellEndDate`                   TIMESTAMP,
	`DiscontinuedDate`              TIMESTAMP,
	`ThumbnailPhotoFileName`        STRING,
	`rowguid`                       STRING,
	`ModifiedDate`                  TIMESTAMP,
	`FlexRowEffectiveFromDate`      TIMESTAMP,
	`FlexRowAuditId`                BIGINT,
	`FlexRowChangeType`             STRING,
	`FlexRowRecordSource`           STRING,
	`FlexRowHash`                   BINARY)
USING DELTA
	LOCATION '${location_path}/awlt/Product'
	PARTITIONED BY (ModifiedDate)
	TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
