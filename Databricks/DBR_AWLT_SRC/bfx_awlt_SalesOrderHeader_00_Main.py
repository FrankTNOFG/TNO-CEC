# Databricks notebook source
dbutils.widgets.text('row_audit_id', '0')
row_audit_id = dbutils.widgets.get('row_audit_id')
dbutils.widgets.text("location_path", "abfss://bfx-plnd@lakehouse.dfs.core.windows.net")
location_path = dbutils.widgets.get("location_path")
dbutils.widgets.text("_bfx_stg", "bfx-stg")
_bfx_stg = dbutils.widgets.get("_bfx_stg")
dbutils.widgets.text("_bfx_ods", "bfx-ods")
_bfx_ods = dbutils.widgets.get("_bfx_ods")
if (spark.sql("SELECT COUNT(*) FROM `" + _bfx_ods + "`.`awlt`.`SalesOrderHeader`").collect()[0][0] == 0):
    dbutils.notebook.run("./bfx_awlt_SalesOrderHeader_01_Full", 0, {"row_audit_id": row_audit_id, "location_path": location_path, "_bfx_stg": _bfx_stg, "_bfx_ods": _bfx_ods})
else:
    dbutils.notebook.run("./bfx_awlt_SalesOrderHeader_02_Delta", 0, {"row_audit_id": row_audit_id, "location_path": location_path, "_bfx_stg": _bfx_stg, "_bfx_ods": _bfx_ods})