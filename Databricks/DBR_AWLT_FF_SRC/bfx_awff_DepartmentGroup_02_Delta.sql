-- Databricks notebook source
-- Delta load of bfx_awff_DepartmentGroup
CREATE WIDGET TEXT row_audit_id DEFAULT "0";
CREATE WIDGET TEXT location_path DEFAULT "abfss://bfx-plnd@lakehouse.dfs.core.windows.net";
CREATE WIDGET TEXT _bfx_ods DEFAULT "bfx-ods";
CREATE WIDGET TEXT _bfx_stg DEFAULT "bfx-stg";


-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_DepartmentGroup`;
CREATE TABLE IF NOT EXISTS `TMP_source_dbo_DepartmentGroup`;

COPY INTO `TMP_source_dbo_DepartmentGroup`
FROM
(
    SELECT   CAST(`DepartmentGroupKey` AS INT) AS `DepartmentGroupKey`
			,CAST(`ParentDepartmentGroupKey` AS INT) AS `ParentDepartmentGroupKey`
			,CAST(`DepartmentGroupName` AS STRING) AS `DepartmentGroupName`
			,`FlexRowEffectiveFromDate` AS `FlexRowEffectiveFromDate`
			,CAST(`FlexRowAuditId` AS BIGINT) AS `FlexRowAuditId`
			,CAST(`FlexRowRecordSource` AS STRING) AS `FlexRowRecordSource`
    FROM    '${location_path}/source_dbo_DepartmentGroup'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_DepartmentGroup_CURRENT`;
CREATE TABLE `TMP_source_dbo_DepartmentGroup_CURRENT` AS 
SELECT	 PSA.`DepartmentGroupKey`
		,PSA.
		,PSA.`FlexRowEffectiveFromDate`
FROM	`${_bfx_ods}``awff`.`DepartmentGroup` PSA
INNER JOIN
(
    SELECT	 CUR.`DepartmentGroupKey`
            ,MAX(CUR.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
    FROM	`${_bfx_ods}`.`awff`.`DepartmentGroup` CUR
    INNER JOIN `TMP_source_dbo_DepartmentGroup` TMP
        ON  CUR.`DepartmentGroupKey` = TMP.`DepartmentGroupKey`
    GROUP BY CUR.`DepartmentGroupKey`
) GRP
    ON  PSA.`DepartmentGroupKey` = GRP.`DepartmentGroupKey`
    AND PSA.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_DepartmentGroup_DELTA`;
CREATE TABLE `TMP_source_dbo_DepartmentGroup_DELTA` AS
SELECT	 STG.`DepartmentGroupKey`
		,STG.`ParentDepartmentGroupKey`
		,STG.`DepartmentGroupName`
		,STG.`FlexRowEffectiveFromDate`
		,STG.`FlexRowAuditId`
		,STG.`FlexRowRecordSource`
		,STG.
FROM
(
	SELECT	 SRC.`DepartmentGroupKey`
			,SRC.`ParentDepartmentGroupKey`
			,SRC.`DepartmentGroupName`
			,SRC.`FlexRowEffectiveFromDate`
			,SRC.`FlexRowAuditId`
			,SRC.`FlexRowRecordSource`
			,SRC.
			,LEAD(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`DepartmentGroupKey`,SRC. ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LEAD_DATE_DIFF`
			,LAG(SRC.`FlexRowEffectiveFromDate`) OVER (PARTITION BY SRC.`DepartmentGroupKey`, SRC. ORDER BY SRC.`FlexRowEffectiveFromDate`) AS `BFX_LAG_DATE_DIFF`
			,COALESCE(LAG(SRC.) OVER (PARTITION BY SRC.`DepartmentGroupKey` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.) AS `BFX_LAG_ROW_DIFF`
			,COALESCE(LEAD(SRC.) OVER (PARTITION BY SRC.`DepartmentGroupKey` ORDER BY SRC.`FlexRowEffectiveFromDate`), SRC.) AS `BFX_LEAD_ROW_DIFF`
	FROM	`TMP_source_dbo_DepartmentGroup` AS SRC
) AS STG
WHERE	STG.`BFX_LAG_DATE_DIFF` IS NULL
	OR (STG. <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_DATE_DIFF` IS NOT NULL)
	OR (STG. <> STG.`BFX_LAG_ROW_DIFF` AND STG.`BFX_LAG_ROW_DIFF` = STG.`BFX_LEAD_ROW_DIFF`); 

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_DepartmentGroup_EXISTS`;
CREATE TABLE `TMP_source_dbo_DepartmentGroup_EXISTS` AS 
SELECT	 STG.`DepartmentGroupKey`
		,STG.
		,STG.`FlexRowEffectiveFromDate`
FROM	`TMP_source_dbo_DepartmentGroup_DELTA` STG
INNER JOIN
(
	SELECT	 SRC.`DepartmentGroupKey`
			,MIN(SRC.`FlexRowEffectiveFromDate`) AS `FlexRowEffectiveFromDate`
	FROM	`TMP_source_dbo_DepartmentGroup_DELTA` SRC
	GROUP BY SRC.`DepartmentGroupKey`
) GRP
	ON	STG.`DepartmentGroupKey` = GRP.`DepartmentGroupKey`
	AND STG.`FlexRowEffectiveFromDate` = GRP.`FlexRowEffectiveFromDate`
INNER JOIN `TMP_source_dbo_DepartmentGroup_CURRENT` CUR
    ON	STG.`DepartmentGroupKey` = CUR.`DepartmentGroupKey`
    AND STG. = CUR.;


-- COMMAND ----------
TRUNCATE TABLE `${_bfx_stg}`.`awff`.`DepartmentGroup`;
INSERT INTO `${_bfx_stg}`.`awff`.`DepartmentGroup`
        (`DepartmentGroupKey`
		,`ParentDepartmentGroupKey`
		,`DepartmentGroupName`
		,`FlexRowEffectiveFromDate`
		,`FlexRowAuditId`
		,`FlexRowRecordSource`)
SELECT   SRC.`DepartmentGroupKey`
		,SRC.`ParentDepartmentGroupKey`
		,SRC.`DepartmentGroupName`
		,SRC.`FlexRowEffectiveFromDate`
		,SRC.`FlexRowAuditId`
		,SRC.`FlexRowRecordSource`
FROM	`TMP_source_dbo_DepartmentGroup_DELTA` SRC
LEFT OUTER JOIN `TMP_source_dbo_DepartmentGroup_CURRENT` TGT
    ON  SRC.`DepartmentGroupKey` = TGT.`DepartmentGroupKey`
LEFT OUTER JOIN `TMP_source_dbo_DepartmentGroup_EXISTS` PSA
    ON  SRC.`DepartmentGroupKey` = PSA.`DepartmentGroupKey`
    AND SRC.`FlexRowEffectiveFromDate` = PSA.`FlexRowEffectiveFromDate`
WHERE   PSA.`DepartmentGroupKey` IS NULL;

-- COMMAND ----------
MERGE INTO `${_bfx_ods}`.`awff`.`DepartmentGroup` AS TGT
USING `${_bfx_stg}`.`awff`.`DepartmentGroup` AS SRC
    ON  SRC.`DepartmentGroupKey` = TGT.`DepartmentGroupKey`
WHEN MATCHED 
    AND	NOT (COALESCE(TRIM(CAST(SRC.`ParentDepartmentGroupKey` AS STRING)), 'NVL') = COALESCE(TRIM(CAST(TGT.`ParentDepartmentGroupKey` AS STRING)), 'NVL')
		AND	COALESCE(TRIM(SRC.`DepartmentGroupName`), 'NVL') = COALESCE(TRIM(TGT.`DepartmentGroupName`), 'NVL'))
    THEN
    UPDATE SET * 
WHEN NOT MATCHED 
    THEN 
    INSERT *;

-- COMMAND ----------
DROP TABLE IF EXISTS `TMP_source_dbo_DepartmentGroup`;
DROP TABLE IF EXISTS `TMP_source_dbo_DepartmentGroup_CURRENT`;
DROP TABLE IF EXISTS `TMP_source_dbo_DepartmentGroup_DELTA`;
DROP TABLE IF EXISTS `TMP_source_dbo_DepartmentGroup_EXISTS`;

