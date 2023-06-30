# Databricks notebook source
dbutils.widgets.text("catalog_name", "bfx-stg")
catalog_name = dbutils.widgets.get('catalog_name')
dbutils.widgets.text("location_path", 'abfss://bfx-stg@lakehouse.dfs.core.windows.net')
location_path = dbutils.widgets.get('location_path')

spark.sql("CREATE SCHEMA IF NOT EXISTS `" + catalog_name + "`.`awff`")
    
dbutils.notebook.run("./awff/awff.Account", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awff/awff.DepartmentGroup", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awff/awff.Finance", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awff/awff.ProductMapping", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awff/awff.Scenario", 0, {"catalog_name": catalog_name, "location_path": location_path})