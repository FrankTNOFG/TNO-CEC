-- Databricks notebook source
-- Delta load of bfx_awff_Scenario
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
DROP TABLE IF EXISTS `TMP_source_dbo_Scenario_CURRENT`;
CREATE TABLE `TMP_source_dbo_Scenario_CURRENT` AS 
SELECT	 PSA.`ScenarioKey`
		,PSA.
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}``awff`.`Scenario` PSA
INNER JOIN
(
    SELECT	 CUR.`ScenarioKey`
            ,MAX(CUR.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
    FROM	`${_bfx_ods}`.`awff`.`Scenario` CUR
    INNER JOIN `TMP_source_dbo_Scenario` TMP
        ON  CUR.`ScenarioKey` = TMP.`ScenarioKey`
    GROUP BY CUR.`ScenarioKey`
) GRP
    ON  PSA.`ScenarioKey` = GRP.`ScenarioKey`
    AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`;

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
DROP TABLE IF EXISTS `TMP_source_dbo_Scenario_EXISTS`;
CREATE TABLE `TMP_source_dbo_Scenario_EXISTS` AS 
SELECT	 STG.`ScenarioKey`
		,STG.
		,STG.`FlexRowEffectiveFromDate`
FROM	`TMP_source_dbo_Scenario_DELTA` STG
INNER JOIN
(
	SELECT	 SRC.`ScenarioKey`
			,MIN(SRC.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`TMP_source_dbo_Scenario_DELTA` SRC
	GROUP BY SRC.`ScenarioKey`
) GRP
	ON	STG.`ScenarioKey` = GRP.`ScenarioKey`
	AND STG.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
INNER JOIN `TMP_source_dbo_Scenario_CURRENT` CUR
    ON	STG.`ScenarioKey` = CUR.`ScenarioKey`
    AND STG. = CUR.;


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
FROM	`TMP_source_dbo_Scenario_DELTA` SRC
LEFT OUTER JOIN `TMP_source_dbo_Scenario_CURRENT` TGT
    ON  SRC.`ScenarioKey` = TGT.`ScenarioKey`
LEFT OUTER JOIN `TMP_source_dbo_Scenario_EXISTS` PSA
    ON  SRC.`ScenarioKey` = PSA.`ScenarioKey`
    AND SRC.`FlexRowEffectiveFromDate` = PSA.`FlexRowEffectiveFromDate`
WHERE   PSA.`ScenarioKey` IS NULL;

-- COMMAND ----------
MERGE INTO `${_bfx_ods}`.`awff`.`Scenario` AS TGT
USING `${_bfx_stg}`.`awff`.`Scenario` AS SRC
    ON  SRC.`ScenarioKey` = TGT.`ScenarioKey`
WHEN MATCHED 
    AND	NOT (COALESCE(TRIM(SRC.`ScenarioName`), 'NVL') = COALESCE(TRIM(TGT.`ScenarioName`), 'NVL')
		AND	COALESCE(TRIM(SRC.`ScenarioDate`), 'NVL') = COALESCE(TRIM(TGT.`ScenarioDate`), 'NVL')
		AND	COALESCE(TRIM(SRC.`ScenarioEmpty`), 'NVL') = COALESCE(TRIM(TGT.`ScenarioEmpty`), 'NVL'))
    THEN
    UPDATE SET * 
WHEN NOT MATCHED 
    THEN 
    INSERT *;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_Scenario`;
DROP TABLE IF EXISTS `TMP_source_dbo_Scenario_CURRENT`;
DROP TABLE IF EXISTS `TMP_source_dbo_Scenario_DELTA`;
DROP TABLE IF EXISTS `TMP_source_dbo_Scenario_EXISTS`;

