CREATE OR REPLACE PROCEDURE SP_ORCHESTRATION_PROCESS_METADATA_APAC_MOD () RETURNS INTEGER
   LANGUAGE nzplsql AS BEGIN_PROC

DECLARE VAR_ORCHESTRATION_PROCESS_ID INT;
VAR_ORCHESTRATION_PROCESS_STEP_ID INT;
VAR_ORCHESTRATION_PROCESS_STEP1_ID INT;
VAR_BATCH_GROUPING_ID INT;
VAR_BATCH_ID INT;
VAR_BATCH_INSTANCE_ID INT;
VAR_JOB_GROUP_ID INT;
VAR_JOB_ID INT;
VAR_JOB_ID_01 INT;
VAR_STEP INT; 
L_ERR_CD				CHAR(5);
L_ERR_MSG				VARCHAR(32000);


BEGIN
SET ISOLATION TO UR;

SET VAR_ORCHESTRATION_PROCESS_ID  = NEXT VALUE FOR SEQUENCE_ORCHESTRATION_PROCESS_METADATA;
SET VAR_ORCHESTRATION_PROCESS_STEP_ID  = NEXT VALUE FOR SEQUENCE_ORCHESTRATION_PROCESS_METADATA;
SET VAR_BATCH_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_BATCH_INSTANCE_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_GROUP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_ID  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO ORCHESTRATION_PROCESS (ORCHESTRATION_PROCESS_ID,ORCHESTRATION_PROCESS_NAME,ORCHESTRATION_PROCESS_DESCRIPTION,INSTANCE_TYPE) VALUES
(VAR_ORCHESTRATION_PROCESS_ID,'APAC Modeling Engine to DIP','APAC Modeling Engine to DIP Data Flow', null);

INSERT INTO ORCHESTRATION_PROCESS_STEP (ORCHESTRATION_PROCESS_STEP_ID,ORCHESTRATION_PROCESS_ID,ORCHESTRATION_PROCESS_STEP_NAME,CHILD_BATCH_ID,TRIGGER_DEPENDENT_STEPS, STARTING_STEP, OPTIONAL_RUN) VALUES
(VAR_ORCHESTRATION_PROCESS_STEP_ID, VAR_ORCHESTRATION_PROCESS_ID,'Process APAC CF in Staging',VAR_BATCH_ID,NULL, true, false);


INSERT INTO BATCH (BATCH_ID,BATCH_NAME,BATCH_DESCRIPTION,APPLICATION_NAME,BATCH_PRIORITY,ON_ERROR,PRE_BATCH_PROCESSING,POST_BATCH_PROCESSING) VALUES
(VAR_BATCH_ID,'Talend APAC Staging Process','Talend APAC Staging Process','TALEND', 1, NULL, NULL, '[POST-COMPLETION]api-dip:checkIpdsAutoTrigger');


INSERT INTO JOB_GROUP (JOB_GROUP_ID,JOB_GROUP_NAME,BATCH_ID,PARENT_JOB_GROUP_ID) VALUES
(VAR_JOB_GROUP_ID,'Talend APAC Staging Process Job Group',VAR_BATCH_ID,NULL);


INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN )
        VALUES ( VAR_JOB_ID, 'Talend APAC Staging Process', NULL, VAR_JOB_GROUP_ID, NULL, NULL );
 

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP, 2, VAR_JOB_ID, 'APAC Staging Load Step ', 1, NULL, 'api-dip:uploadinsert','api-diptalend:apac_cashflow_staging_load',NULL);
 
INSERT INTO STAGING_LOAD_STEP ( STEP_ID, STAGING_SCHEMA_NAME, STAGING_TABLE_NAME, TRUNCATE_STAGING_TABLE, MANDATORY_FILE, FILE_HEADER_ROWS, DELIMITER, CONVERTED_STAGING_TABLE_NAME, CHECK_DUPLICATES, SPECIFIC_PROCESS, DATA_FILE_TREATMENT, REGIONAL_DATA, FILE_DIRECTORY, FILE_NAME_PATTERN ) 
        VALUES ( VAR_STEP, 'STAGING_${ENV}', '', 0, 0, 0, '~', NULL, 0, NULL, NULL, 0, NULL, NULL );


SELECT BATCH_GROUPING_ID INTO VAR_BATCH_GROUPING_ID FROM BATCH_GROUPING WHERE BATCH_GROUPING_NAME = 'LIFE MODELING PROCESSING';

INSERT INTO EVENT ( EVENT_CODE, EVENT_DESCRIPTION )
        VALUES ( 'EVT_INGEST_APAC_CASHFLOWS_FOR_VALIDATION', 'Event for APAC Staging Load' );
	
INSERT INTO EVENT_MAPPING ( EVENT_CODE, BATCH_ID, ORCHESTRATION_PROCESS_STEP_ID, ORCHESTRATION_PROCESS_ID, BATCH_GROUPING_ID, IS_DWH_SWITCH)
        VALUES ( 'EVT_INGEST_APAC_CASHFLOWS_FOR_VALIDATION', VAR_BATCH_ID, VAR_ORCHESTRATION_PROCESS_STEP_ID, VAR_ORCHESTRATION_PROCESS_ID, VAR_BATCH_GROUPING_ID, FALSE);
		

SET VAR_ORCHESTRATION_PROCESS_STEP1_ID = NEXT VALUE FOR SEQUENCE_ORCHESTRATION_PROCESS_METADATA;
SET VAR_BATCH_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_BATCH_INSTANCE_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_GROUP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_ID_01 = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO JOB_DEPENDENCY (JOB_ID, PARENT_JOB_ID) VALUES(VAR_JOB_ID_01, VAR_JOB_ID);

INSERT INTO ORCHESTRATION_PROCESS_STEP (ORCHESTRATION_PROCESS_STEP_ID,ORCHESTRATION_PROCESS_ID,ORCHESTRATION_PROCESS_STEP_NAME,CHILD_BATCH_ID,TRIGGER_DEPENDENT_STEPS, STARTING_STEP, OPTIONAL_RUN) 
	VALUES (VAR_ORCHESTRATION_PROCESS_STEP1_ID, VAR_ORCHESTRATION_PROCESS_ID,'Process APAC CF in RDWH', VAR_BATCH_ID, NULL, false, false);

INSERT INTO ORCHESTRATION_PROCESS_STEP_DEPENDENCY(ORCHESTRATION_PROCESS_STEP_ID, PARENT_ORCHESTRATION_PROCESS_STEP_ID)
	VALUES (VAR_ORCHESTRATION_PROCESS_STEP1_ID, VAR_ORCHESTRATION_PROCESS_STEP_ID);

INSERT INTO BATCH (BATCH_ID,BATCH_NAME,BATCH_DESCRIPTION,APPLICATION_NAME,BATCH_PRIORITY,ON_ERROR,PRE_BATCH_PROCESSING,POST_BATCH_PROCESSING) VALUES
	(VAR_BATCH_ID,'Talend APAC DWH Process','Talend APAC DWH Process','TALEND', 1,NULL,NULL,NULL);

INSERT INTO JOB_GROUP (JOB_GROUP_ID,JOB_GROUP_NAME,BATCH_ID,PARENT_JOB_GROUP_ID) VALUES
	(VAR_JOB_GROUP_ID,'Talend APAC DWH Process Job Group',VAR_BATCH_ID,NULL);

INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN )
        VALUES ( VAR_JOB_ID, 'Talend APAC DWH Process', NULL, VAR_JOB_GROUP_ID, NULL, NULL);


SET VAR_STEP = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP, 3, VAR_JOB_ID, 'INSERT APAC CF in RDWH', 1, NULL, 'api-dip:uploadupdate','api-diptalend:apac_cashflow_dwh_load',NULL);
 
INSERT INTO DWH_LOAD_STEP ( STEP_ID, DWH_SCHEMA_NAME, DWH_TABLE_NAME, STAGING_SCHEMA_NAME, STAGING_TABLE_NAME, INGESTION_MODE, HISTORIZATION_TYPE, SPECIFIC_PROCESS ) 
        VALUES ( VAR_STEP, NULL, NULL, NULL, NULL, NULL, NULL, NULL );


INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN )
        VALUES ( VAR_JOB_ID_01, 'Allocation Process Post IPDS for APAC', NULL, VAR_JOB_GROUP_ID, NULL, NULL);


SET VAR_STEP = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP, 3, VAR_JOB_ID_01, 'Talend call for Allocation Process Post IPDS for APAC', 1, NULL, NULL, 'api-diptalend:lrm_ra_allocation_histo_processing', NULL);
 
INSERT INTO DWH_LOAD_STEP ( STEP_ID, DWH_SCHEMA_NAME, DWH_TABLE_NAME, STAGING_SCHEMA_NAME, STAGING_TABLE_NAME, INGESTION_MODE, HISTORIZATION_TYPE, SPECIFIC_PROCESS ) 
        VALUES ( VAR_STEP, NULL, NULL, NULL, NULL, NULL, NULL, NULL );

INSERT INTO EVENT ( EVENT_CODE, EVENT_DESCRIPTION )
        VALUES ( 'EVT_APAC_CASHFLOWS_VALIDATED', 'Event to Process APAC CF in RDWH' );

INSERT INTO EVENT_MAPPING ( EVENT_CODE, BATCH_ID, ORCHESTRATION_PROCESS_STEP_ID, ORCHESTRATION_PROCESS_ID, BATCH_GROUPING_ID, IS_DWH_SWITCH)
        VALUES ( 'EVT_APAC_CASHFLOWS_VALIDATED', VAR_BATCH_ID, VAR_ORCHESTRATION_PROCESS_STEP1_ID, NULL, VAR_BATCH_GROUPING_ID, TRUE);

EXCEPTION WHEN OTHERS THEN 
	L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD;
 
END; 

END_PROC;