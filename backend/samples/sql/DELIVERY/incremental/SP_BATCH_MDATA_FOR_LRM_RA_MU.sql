CREATE OR REPLACE PROCEDURE SP_BATCH_METADATA_FOR_LRM_RA_MU () RETURNS INTEGER
   LANGUAGE nzplsql AS BEGIN_PROC

DECLARE VAR_BATCH_GROUPING_ID INT ;
VAR_BATCH_ID INT;
VAR_JOB_GROUP_ID INT;
VAR_JOB_ID INT;
VAR_JOB_ID_01 INT;
VAR_STEP INT;
L_ERR_CD CHAR(5);
L_ERR_MSG VARCHAR(32000);

BEGIN
SET ISOLATION TO UR;

SET VAR_BATCH_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_GROUP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_ID  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_ID_01  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO JOB_DEPENDENCY (JOB_ID, PARENT_JOB_ID) VALUES(VAR_JOB_ID_01, VAR_JOB_ID);

INSERT INTO BATCH (BATCH_ID,BATCH_NAME,BATCH_DESCRIPTION,APPLICATION_NAME,BATCH_PRIORITY,ON_ERROR,PRE_BATCH_PROCESSING,POST_BATCH_PROCESSING)
	VALUES (VAR_BATCH_ID, 'Manual Upload for Life Risk Adjustment', 'Manual Upload for Life Risk Adjustment', 'DIP', 1, NULL, NULL, NULL);

INSERT INTO JOB_GROUP (JOB_GROUP_ID,JOB_GROUP_NAME,BATCH_ID,PARENT_JOB_GROUP_ID)
	VALUES (VAR_JOB_GROUP_ID, 'Manual Upload for Life Risk Adjustment Job Group', VAR_BATCH_ID, NULL);

INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN )
	VALUES (VAR_JOB_ID, 'Manual Upload for Life Risk Adjustment job', NULL, VAR_JOB_GROUP_ID, NULL, NULL);

SET VAR_STEP = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
	VALUES (VAR_STEP, 3, VAR_JOB_ID, 'Manual Upload for Life Risk Adjustment Talend Processing', 1, NULL, NULL, 'api-diptalend:mu_life_ra_processing', NULL);

INSERT INTO DWH_LOAD_STEP ( STEP_ID, DWH_SCHEMA_NAME, DWH_TABLE_NAME, STAGING_SCHEMA_NAME, STAGING_TABLE_NAME, INGESTION_MODE
, HISTORIZATION_TYPE, SPECIFIC_PROCESS ) 
        VALUES (VAR_STEP, NULL, NULL, NULL, NULL, NULL, NULL, NULL );


INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN )
        VALUES ( VAR_JOB_ID_01, 'Allocation Process Post MU Life RA', NULL, VAR_JOB_GROUP_ID, NULL, NULL);

SET VAR_STEP = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP, 3, VAR_JOB_ID_01, 'Talend call for Allocation Process post MU Life RA', 1, NULL, NULL, 'api-diptalend:lrm_ra_allocation_frame1_processing', NULL);
 
INSERT INTO DWH_LOAD_STEP ( STEP_ID, DWH_SCHEMA_NAME, DWH_TABLE_NAME, STAGING_SCHEMA_NAME, STAGING_TABLE_NAME, INGESTION_MODE, HISTORIZATION_TYPE, SPECIFIC_PROCESS ) 
        VALUES ( VAR_STEP, NULL, NULL, NULL, NULL, NULL, NULL, NULL );

SELECT BATCH_GROUPING_ID INTO VAR_BATCH_GROUPING_ID FROM BATCH_GROUPING WHERE BATCH_GROUPING_NAME = 'LRM RA PROCESSING';

IF VAR_BATCH_GROUPING_ID IS NULL THEN 
	SET VAR_BATCH_GROUPING_ID = NEXT VALUE FOR SEQUENCE_BATCH_GROUPING;

	INSERT INTO BATCH_GROUPING (BATCH_GROUPING_ID, BATCH_GROUPING_NAME, ACTIVE_DWH, PASSIVE_DWH, LAST_UPDATE_TIME)
		VALUES(VAR_BATCH_GROUPING_ID, 'LRM RA PROCESSING', 'DWHD2_' || chr(36) || '{ENV}', 'DWHD1_' || chr(36) || '{ENV}', CURRENT_TIMESTAMP);

END IF;

INSERT INTO EVENT ( EVENT_CODE, EVENT_DESCRIPTION )
	VALUES ('EVT_MU_LIFE_RISK_ADJUSTMENT', 'Event for MU for Life Risk Adjustment' );

INSERT INTO EVENT_MAPPING ( EVENT_CODE, BATCH_ID, ORCHESTRATION_PROCESS_STEP_ID, ORCHESTRATION_PROCESS_ID, BATCH_GROUPING_ID, IS_DWH_SWITCH, DELIMITER)
	VALUES ('EVT_MU_LIFE_RISK_ADJUSTMENT', VAR_BATCH_ID, NULL, NULL, VAR_BATCH_GROUPING_ID, FALSE, '~');

EXCEPTION WHEN OTHERS THEN 
	L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD; 
 
END; 

END_PROC;