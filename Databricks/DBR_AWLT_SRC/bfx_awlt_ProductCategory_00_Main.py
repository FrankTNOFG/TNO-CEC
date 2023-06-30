# Databricks notebook source
dbutils.widgets.text('row_audit_id', '0')
row_audit_id = dbutils.widgets.get('row_audit_id')
dbutils.widgets.text("location_path", "abfss://bfx-plnd@lakehouse.dfs.core.windows.net")
location_path = dbutils.widgets.get("location_path")
dbutils.widgets.text("_bfx_stg", "bfx-stg")
_bfx_stg = dbutils.widgets.get("_bfx_stg")dbutils.notebook.run("./bfx_awlt_ProductCategory_01_Full", 0, {"row_audit_id": row_audit_id, "location_path": location_path, "_bfx-stg": _bfx-stg})