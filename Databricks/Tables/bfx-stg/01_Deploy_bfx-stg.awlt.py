# Databricks notebook source
dbutils.widgets.text("catalog_name", "bfx-stg")
catalog_name = dbutils.widgets.get('catalog_name')
dbutils.widgets.text("location_path", 'abfss://bfx-stg@lakehouse.dfs.core.windows.net')
location_path = dbutils.widgets.get('location_path')

spark.sql("CREATE SCHEMA IF NOT EXISTS `" + catalog_name + "`.`awlt`")
    
dbutils.notebook.run("./awlt/awlt.Address", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awlt/awlt.Customer", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awlt/awlt.CustomerAddress", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awlt/awlt.Product", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awlt/awlt.ProductCategory", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awlt/awlt.ProductDescription", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awlt/awlt.ProductModel", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awlt/awlt.ProductModelProductDescription", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awlt/awlt.SalesOrderDetail", 0, {"catalog_name": catalog_name, "location_path": location_path})
dbutils.notebook.run("./awlt/awlt.SalesOrderHeader", 0, {"catalog_name": catalog_name, "location_path": location_path})