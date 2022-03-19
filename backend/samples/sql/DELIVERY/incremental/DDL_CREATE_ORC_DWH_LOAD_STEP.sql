DROP TABLE DWH_LOAD_STEP IF EXISTS;
CREATE TABLE DWH_LOAD_STEP
(
	STEP_ID	                      INT NOT NULL,
	DWH_SCHEMA_NAME	              VARCHAR(128),
	DWH_TABLE_NAME	              VARCHAR(128),
	STAGING_SCHEMA_NAME	          VARCHAR(128),
	STAGING_TABLE_NAME	          VARCHAR(128),
	INGESTION_MODE	              VARCHAR(16),
	HISTORIZATION_TYPE	          VARCHAR(16),
	SPECIFIC_PROCESS              VARCHAR(10000)
) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
ALTER TABLE DWH_LOAD_STEP ADD CONSTRAINT DWH_LOAD_STEP_PK PRIMARY KEY (STEP_ID);