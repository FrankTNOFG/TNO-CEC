# Databricks notebook source
dbutils.widgets.text("catalog_name", "bfx-stg")
catalog_name = dbutils.widgets.get("catalog_name")
dbutils.widgets.text("location_path", 'abfss://bfx-stg@lakehouse.dfs.core.windows.net')
location_path = dbutils.widgets.get('location_path')

dbutils.notebook.run("./01_Deploy_bfx-stg.awff", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./01_Deploy_bfx-stg.awlt", 0, {"catalog_name": catalog_name, "location_path": location_path})