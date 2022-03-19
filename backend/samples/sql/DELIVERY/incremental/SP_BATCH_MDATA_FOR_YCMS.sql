CREATE OR REPLACE PROCEDURE SP_BATCH_METADATA_FOR_YCMS () RETURNS INTEGER
   LANGUAGE nzplsql AS BEGIN_PROC

DECLARE VAR_BUSINESS_EVENT_ID_01 INT ;
VAR_BUSINESS_EVENT_ID_02 INT ;
VAR_BUSINESS_PROCESS_ID_01 INT ;
VAR_BUSINESS_PROCESS_ID_02 INT ;
VAR_BUSINESS_PROCESS_STEP_ID_01 INT ; 
VAR_BUSINESS_PROCESS_STEP_ID_02 INT ;
VAR_BATCH_GROUPING_ID INT ;
VAR_BATCH_ID INT;
VAR_JOB_GROUP_ID INT;
VAR_JOB_ID INT;
VAR_JOB_ID_CHILD INT;
VAR_STEP_ID INT;
L_ERR_CD CHAR(5);
L_ERR_MSG VARCHAR(32000);

BEGIN
SET ISOLATION TO UR;

SET VAR_BUSINESS_EVENT_ID_01 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;
SET VAR_BUSINESS_EVENT_ID_02 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;
SET VAR_BUSINESS_PROCESS_ID_01 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;
SET VAR_BUSINESS_PROCESS_ID_02 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;
SET VAR_BUSINESS_PROCESS_STEP_ID_01 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;
SET VAR_BUSINESS_PROCESS_STEP_ID_02 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;


INSERT INTO BUSINESS_EVENT (BUSINESS_EVENT_ID, EVENT_CODE, CLOSING_TYPE_CODE, SCENARIO_TYPE, RUN_TYPE, IS_ACTIVE, TRIGGER_MODE, DELAY_DAYS)
	VALUES(VAR_BUSINESS_EVENT_ID_01, 'EVT_INGEST_YIELD_CURVE', 'I17G', 'CLOSING', 'POSTING', true, 'External System Triggered', NULL);

INSERT INTO BUSINESS_EVENT (BUSINESS_EVENT_ID, EVENT_CODE, CLOSING_TYPE_CODE, SCENARIO_TYPE, RUN_TYPE, IS_ACTIVE, TRIGGER_MODE, DELAY_DAYS)
	VALUES(VAR_BUSINESS_EVENT_ID_02, 'EVT_INGEST_YIELD_CURVE', 'I17L', 'CLOSING', 'POSTING', true, 'External System Triggered', NULL);

INSERT INTO BUSINESS_PROCESS (BUSINESS_PROCESS_ID, BUSINESS_PROCESS_NAME, CLOSING_TYPE_CODE, SCENARIO_TYPE, RUN_TYPE)
	VALUES(VAR_BUSINESS_PROCESS_ID_01, 'Business Process for YCMS to DIP Data Flow', 'I17G', 'CLOSING', 'POSTING');

INSERT INTO BUSINESS_PROCESS (BUSINESS_PROCESS_ID, BUSINESS_PROCESS_NAME, CLOSING_TYPE_CODE, SCENARIO_TYPE, RUN_TYPE)
	VALUES(VAR_BUSINESS_PROCESS_ID_02, 'Business Process for YCMS to DIP Data Flow', 'I17L', 'CLOSING', 'POSTING');

INSERT INTO BUSINESS_PROCESS_STEP (BUSINESS_PROCESS_STEP_ID, BUSINESS_PROCESS_STEP_NAME, BUSINESS_PROCESS_ID, STEP_SEQUENCE)
	VALUES(VAR_BUSINESS_PROCESS_STEP_ID_01, 'YCMS to DIP Data Flow', VAR_BUSINESS_PROCESS_ID_01, 1);
	
INSERT INTO BUSINESS_PROCESS_STEP (BUSINESS_PROCESS_STEP_ID, BUSINESS_PROCESS_STEP_NAME, BUSINESS_PROCESS_ID, STEP_SEQUENCE)
	VALUES(VAR_BUSINESS_PROCESS_STEP_ID_02, 'YCMS to DIP Data Flow', VAR_BUSINESS_PROCESS_ID_02, 1);

INSERT INTO BUSINESS_PROCESS_STEP_EVENT_MAPPING (BUSINESS_PROCESS_STEP_ID, BUSINESS_EVENT_ID)
	VALUES(VAR_BUSINESS_PROCESS_STEP_ID_01, VAR_BUSINESS_EVENT_ID_01);

INSERT INTO BUSINESS_PROCESS_STEP_EVENT_MAPPING (BUSINESS_PROCESS_STEP_ID, BUSINESS_EVENT_ID)
	VALUES(VAR_BUSINESS_PROCESS_STEP_ID_02, VAR_BUSINESS_EVENT_ID_02);


SET VAR_BATCH_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_GROUP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO BATCH (BATCH_ID,BATCH_NAME,BATCH_DESCRIPTION,APPLICATION_NAME,BATCH_PRIORITY,ON_ERROR,PRE_BATCH_PROCESSING,POST_BATCH_PROCESSING)
	VALUES (VAR_BATCH_ID,'YCMS to DIP Data flow','YCMS to DIP Data flow','DIP', 1,NULL,NULL,NULL);

INSERT INTO JOB_GROUP (JOB_GROUP_ID,JOB_GROUP_NAME,BATCH_ID,PARENT_JOB_GROUP_ID)
	VALUES (VAR_JOB_GROUP_ID,'YCMS to DIP Data flow Job Group',VAR_BATCH_ID,NULL);

INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN )
	VALUES ( VAR_JOB_ID, 'YCMS to DIP Data flow Job', NULL, VAR_JOB_GROUP_ID, NULL, NULL);

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_ID, 2, VAR_JOB_ID, 'YCMS to DIP Staging Load Step', 1, NULL, 'api-dip:uploadinsertycms', 'db-dip:sp-explicit', 'api-dip:ycmsPostProcessing');
 
INSERT INTO STAGING_LOAD_STEP ( STEP_ID, STAGING_SCHEMA_NAME, STAGING_TABLE_NAME, TRUNCATE_STAGING_TABLE, MANDATORY_FILE, FILE_HEADER_ROWS, DELIMITER, CONVERTED_STAGING_TABLE_NAME, CHECK_DUPLICATES, SPECIFIC_PROCESS, DATA_FILE_TREATMENT, REGIONAL_DATA, FILE_DIRECTORY, FILE_NAME_PATTERN ) 
        VALUES ( VAR_STEP_ID, NULL, NULL, NULL, NULL, NULL, NULL
		, NULL, NULL, 'STAGING_' || chr(36) || '{ENV}.SP_LOAD_YCMS_ECONOMIC_RATE(''' || chr(36) || '{UPLOAD_REQUEST_ID}'', ''STAGING_' || chr(36) || '{ENV}'', ''YCMS_OUTPUTS_FWD'', ''STAGING_' || chr(36) || '{ENV}'', ''WRK_ECONOMIC_RATE'')', NULL, NULL, NULL, NULL );

SET VAR_STEP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING, STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_ID, 3, VAR_JOB_ID, 'YCMS to DIP DWH Processing Step', 2, NULL, NULL, 'db-dip:sp-explicit', NULL);

INSERT INTO DWH_LOAD_STEP (STEP_ID, DWH_SCHEMA_NAME, DWH_TABLE_NAME, STAGING_SCHEMA_NAME, STAGING_TABLE_NAME, INGESTION_MODE, HISTORIZATION_TYPE
, SPECIFIC_PROCESS)
		VALUES(VAR_STEP_ID, NULL, NULL, NULL, NULL, NULL, NULL
		, chr(36) || '{DWH_DB}.GP_HISTORIZATION(''' || chr(36) || '{UPLOAD_REQUEST_ID}'', ''INCREMENTAL'', ''STAGING_' || chr(36) || '{ENV}'', ''WRK_ECONOMIC_RATE'', ''' || chr(36) || '{DWH_DB}'', ''DWH_ECONOMIC_RATE'', ''DELIVERY_' || chr(36) || '{ENV}'', ''CONF_TABLE_KEY'', FALSE)');

SET VAR_JOB_ID_CHILD = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
 
INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN )
	VALUES ( VAR_JOB_ID_CHILD, 'Post YCMS FFX Process', NULL, VAR_JOB_GROUP_ID, NULL, NULL);

INSERT INTO JOB_DEPENDENCY (JOB_ID, PARENT_JOB_ID)
		VALUES(VAR_JOB_ID_CHILD, VAR_JOB_ID);

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_ID, 3, VAR_JOB_ID_CHILD, 'Talend call for FFX Process Post YCMS', 1, NULL, 'api-dip:uploadinsert','api-diptalend:mu_ffx_processing',NULL);

INSERT INTO DWH_LOAD_STEP ( STEP_ID, DWH_SCHEMA_NAME, DWH_TABLE_NAME, STAGING_SCHEMA_NAME, STAGING_TABLE_NAME, INGESTION_MODE, HISTORIZATION_TYPE, SPECIFIC_PROCESS ) 
        VALUES ( VAR_STEP_ID, NULL, NULL, NULL, NULL, NULL, NULL, NULL );

SELECT BATCH_GROUPING_ID INTO VAR_BATCH_GROUPING_ID FROM BATCH_GROUPING WHERE BATCH_GROUPING_NAME = 'YIELD CURVE PROCESSING';

INSERT INTO EVENT ( EVENT_CODE, EVENT_DESCRIPTION )
	VALUES ( 'EVT_INGEST_YIELD_CURVE', 'Event for YCMS to DIP Data flow' );

INSERT INTO EVENT_MAPPING ( EVENT_CODE, BATCH_ID, ORCHESTRATION_PROCESS_STEP_ID, ORCHESTRATION_PROCESS_ID, BATCH_GROUPING_ID, IS_DWH_SWITCH)
	VALUES ( 'EVT_INGEST_YIELD_CURVE', VAR_BATCH_ID, NULL, NULL, VAR_BATCH_GROUPING_ID, FALSE);


EXCEPTION WHEN OTHERS THEN 
	L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD; 
 
END; 

END_PROC;