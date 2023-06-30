-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT "bfx-stg";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-stg@lakehouse.dfs.core.windows.net";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS `${catalog_name}`.`awlt`.`SalesOrderHeader`(
	`SalesOrderID`                  INT	NOT NULL,
	`RevisionNumber`                TINYINT,
	`OrderDate`                     TIMESTAMP,
	`DueDate`                       TIMESTAMP,
	`ShipDate`                      TIMESTAMP,
	`Status`                        TINYINT,
	`OnlineOrderFlag`               BOOLEAN,
	`SalesOrderNumber`              STRING,
	`PurchaseOrderNumber`           STRING,
	`AccountNumber`                 STRING,
	`CustomerID`                    INT,
	`ShipToAddressID`               INT,
	`BillToAddressID`               INT,
	`ShipMethod`                    STRING,
	`CreditCardApprovalCode`        STRING,
	`SubTotal`                      DOUBLE,
	`TaxAmt`                        DOUBLE,
	`Freight`                       DOUBLE,
	`TotalDue`                      DOUBLE,
	`Comment`                       STRING,
	`rowguid`                       STRING,
	`ModifiedDate`                  TIMESTAMP,
	`FlexRowEffectiveFromDate`      TIMESTAMP,
	`FlexRowAuditId`                BIGINT,
	`FlexRowChangeType`             STRING,
	`FlexRowRecordSource`           STRING,
	`FlexRowHash`                   BINARY)
USING DELTA
	LOCATION '${location_path}/awlt/SalesOrderHeader'
	TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
