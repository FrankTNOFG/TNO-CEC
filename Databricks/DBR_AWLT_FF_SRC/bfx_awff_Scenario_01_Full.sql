-- Databricks notebook source
-- Full load notebook bfx_awff_Scenario
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Scenario`;
CREATE TABLE IF NOT EXISTS `TMP_source_dbo_Scenario`;

COPY INTO `TMP_source_dbo_Scenario`
FROM
(
    SELECT   CAST(`ScenarioKey` AS INT) AS `ScenarioKey`
			,CAST(`ScenarioName` AS STRING) AS `ScenarioName`
			,CAST(`ScenarioDate` AS STRING) AS `ScenarioDate`
			,CAST(`ScenarioEmpty` AS STRING) AS `ScenarioEmpty`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/source_dbo_Scenario'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Scenario_DELTA`;
CREATE TABLE `TMP_source_dbo_Scenario_DELTA` AS
SELECT	 STG.`ScenarioKey`
		,STG.`ScenarioName`
		,STG.`ScenarioDate`
		,STG.`ScenarioEmpty`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowRecordSource`
		,STG.
FROM
(
	SELECT	 SRC.`ScenarioKey`
			,SRC.`ScenarioName`
			,SRC.`ScenarioDate`
			,SRC.`ScenarioEmpty`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowRecordSource`
			,SRC.
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`ScenarioKey`,SRC. ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`ScenarioKey`, SRC. ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.) OVER (PARTITION BY SRC.`ScenarioKey` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.) OVER (PARTITION BY SRC.`ScenarioKey` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_source_dbo_Scenario` AS SRC
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG. <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG. <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awff`.`Scenario`;
INSERT INTO `${_bfx_stg}`.`awff`.`Scenario`
        (`ScenarioKey`
		,`ScenarioName`
		,`ScenarioDate`
		,`ScenarioEmpty`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT   SRC.`ScenarioKey`
		,SRC.`ScenarioName`
		,SRC.`ScenarioDate`
		,SRC.`ScenarioEmpty`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,SRC.`FlexRowRecordSource`
FROM	`TMP_source_dbo_Scenario_DELTA` SRC;

-- COMMAND ----------
INSERT INTO `${_bfx_ods}`.`awff`.`Scenario`
        (`ScenarioKey`
		,`ScenarioName`
		,`ScenarioDate`
		,`ScenarioEmpty`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT   INS.`ScenarioKey`
		,INS.`ScenarioName`
		,INS.`ScenarioDate`
		,INS.`ScenarioEmpty`
		,INS.`FlexRowEffectiveFromDate`
		,INS.`FlexRowAuditId`
		,INS.`FlexRowRecordSource`
FROM	`${_bfx_stg}`.`awff`.`Scenario` INS;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Scenario`;
DROP TABLE IF EXISTS `TMP_source_dbo_Scenario_DELTA`;

