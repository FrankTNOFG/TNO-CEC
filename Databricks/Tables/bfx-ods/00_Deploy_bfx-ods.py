# Databricks notebook source
dbutils.widgets.text("catalog_name", "bfx-ods")
catalog_name = dbutils.widgets.get("catalog_name")
dbutils.widgets.text("location_path", 'abfss://bfx-ods@lakehouse.dfs.core.windows.net')
location_path = dbutils.widgets.get('location_path')

dbutils.notebook.run("./01_Deploy_bfx-ods.awff", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./01_Deploy_bfx-ods.awlt", 0, {"catalog_name": catalog_name, "location_path": location_path})