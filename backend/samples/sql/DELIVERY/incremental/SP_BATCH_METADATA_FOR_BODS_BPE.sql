CREATE OR REPLACE PROCEDURE SP_BATCH_METADATA_FOR_BODS_BPE () RETURNS INTEGER
   LANGUAGE nzplsql AS BEGIN_PROC

DECLARE VAR_BATCH_GROUPING_ID INT ;
VAR_BATCH_ID INT;
VAR_JOB_GROUP_ID INT;
VAR_JOB_ID INT;
VAR_STEP INT; 
VAR_BUSINESS_EVENT_ID_01 INT ;
VAR_BUSINESS_EVENT_ID_02 INT ;
VAR_BUSINESS_PROCESS_ID_01 INT ;
VAR_BUSINESS_PROCESS_ID_02 INT ;
VAR_BUSINESS_PROCESS_STEP_ID_01 INT ;
VAR_BUSINESS_PROCESS_STEP_ID_02 INT ;
L_ERR_CD				CHAR(5);
L_ERR_MSG				VARCHAR(32000);


BEGIN
SET ISOLATION TO UR;

SET VAR_BUSINESS_EVENT_ID_01 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;
SET VAR_BUSINESS_EVENT_ID_02 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;
SET VAR_BUSINESS_PROCESS_STEP_ID_01 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;
SET VAR_BUSINESS_PROCESS_STEP_ID_02 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;

INSERT INTO BUSINESS_EVENT (BUSINESS_EVENT_ID, EVENT_CODE, CLOSING_TYPE_CODE, SCENARIO_TYPE, RUN_TYPE, IS_ACTIVE, TRIGGER_MODE, DELAY_DAYS)
VALUES(VAR_BUSINESS_EVENT_ID_01, 'EVT_DIP_TO_BPE_FOR_SAS_RESULTS', 'I17G', 'CLOSING', 'POSTING', true, 'External System Triggered', NULL);

INSERT INTO BUSINESS_EVENT (BUSINESS_EVENT_ID, EVENT_CODE, CLOSING_TYPE_CODE, SCENARIO_TYPE, RUN_TYPE, IS_ACTIVE, TRIGGER_MODE, DELAY_DAYS)
VALUES(VAR_BUSINESS_EVENT_ID_02, 'EVT_DIP_TO_BPE_FOR_SAS_RESULTS', 'I17L', 'CLOSING', 'POSTING', true, 'External System Triggered', NULL);

SELECT BUSINESS_PROCESS_ID INTO VAR_BUSINESS_PROCESS_ID_01 FROM BUSINESS_PROCESS 
	WHERE BUSINESS_PROCESS_NAME = 'Business Process for DIP to SAS to DIP and to Omega Data Flow' AND CLOSING_TYPE_CODE = 'I17G';

SELECT BUSINESS_PROCESS_ID INTO VAR_BUSINESS_PROCESS_ID_02 FROM BUSINESS_PROCESS 
	WHERE BUSINESS_PROCESS_NAME = 'Business Process for DIP to SAS to DIP and to Omega Data Flow' AND CLOSING_TYPE_CODE = 'I17L';

INSERT INTO BUSINESS_PROCESS_STEP (BUSINESS_PROCESS_STEP_ID, BUSINESS_PROCESS_STEP_NAME, BUSINESS_PROCESS_ID, STEP_SEQUENCE)
VALUES(VAR_BUSINESS_PROCESS_STEP_ID_01, 'DIP to BPE (SAS Results) Data Flow', VAR_BUSINESS_PROCESS_ID_01, 3);

INSERT INTO BUSINESS_PROCESS_STEP (BUSINESS_PROCESS_STEP_ID, BUSINESS_PROCESS_STEP_NAME, BUSINESS_PROCESS_ID, STEP_SEQUENCE)
VALUES(VAR_BUSINESS_PROCESS_STEP_ID_02, 'DIP to BPE (SAS Results) Data Flow', VAR_BUSINESS_PROCESS_ID_02, 3);

INSERT INTO BUSINESS_PROCESS_STEP_EVENT_MAPPING (BUSINESS_PROCESS_STEP_ID, BUSINESS_EVENT_ID)
VALUES(VAR_BUSINESS_PROCESS_STEP_ID_01, VAR_BUSINESS_EVENT_ID_01);

INSERT INTO BUSINESS_PROCESS_STEP_EVENT_MAPPING (BUSINESS_PROCESS_STEP_ID, BUSINESS_EVENT_ID)
VALUES(VAR_BUSINESS_PROCESS_STEP_ID_02, VAR_BUSINESS_EVENT_ID_02);

UPDATE BATCH
	SET POST_BATCH_PROCESSING = 'api-dip:postSasResultsProcessing'
	WHERE POST_BATCH_PROCESSING = 'api-dip:dipToOmegaForSasResults' AND BATCH_NAME = 'SAS to DIP Run';

SET VAR_BATCH_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_GROUP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_ID  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO BATCH (BATCH_ID,BATCH_NAME,BATCH_DESCRIPTION,APPLICATION_NAME,BATCH_PRIORITY,ON_ERROR,PRE_BATCH_PROCESSING,POST_BATCH_PROCESSING)
	VALUES (VAR_BATCH_ID,'DIP to BPE (SAS Results) Data flow', 'DIP to BPE (SAS Results) Data flow', 'DIP', 1, NULL, NULL, NULL);

INSERT INTO JOB_GROUP (JOB_GROUP_ID,JOB_GROUP_NAME,BATCH_ID,PARENT_JOB_GROUP_ID)
	VALUES (VAR_JOB_GROUP_ID, 'DIP to BPE (SAS Results) Data flow Job Group', VAR_BATCH_ID, NULL);

INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN )
	VALUES ( VAR_JOB_ID, 'DIP to BPE (SAS Results) Data flow Job', NULL, VAR_JOB_GROUP_ID, NULL, NULL);

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP, 4, VAR_JOB_ID, 'DIP to BPE (SAS Results) Data flow', 1, NULL, NULL, 'api-dip:dipToBpeSasResults', NULL);
		
INSERT INTO DATA_EXPOSITION_STEP (STEP_ID, SCHEMA_NAME, TABLE_NAME, DELIMITER, INCLUDE_HEADER, SPECIFIC_PROCESS, FILE_DIRECTORY, FILE_NAME_PATTERN, DATA_FILE_TREATMENT)
		VALUES(VAR_STEP, NULL, NULL, NULL, NULL, NULL, 'bpe/out', chr(36) || '{E}_ESIJ0790_AE_' || chr(36) || '{SSD_AE}_' || chr(36) || '{ESB_AE}_CSMENGINE_' || chr(36) || '{TIMESTAMP}.dat', 'ARCHIVE');

SELECT BATCH_GROUPING_ID INTO VAR_BATCH_GROUPING_ID FROM BATCH_GROUPING WHERE BATCH_GROUPING_NAME = 'SAS to DIP PROCESSING';

INSERT INTO EVENT ( EVENT_CODE, EVENT_DESCRIPTION )
        VALUES ( 'EVT_DIP_TO_BPE_FOR_SAS_RESULTS', 'Event to extract SAS Results to BPE (BODS)' );

INSERT INTO EVENT_MAPPING ( EVENT_CODE, BATCH_ID, ORCHESTRATION_PROCESS_STEP_ID, ORCHESTRATION_PROCESS_ID, BATCH_GROUPING_ID, IS_DWH_SWITCH)
        VALUES ( 'EVT_DIP_TO_BPE_FOR_SAS_RESULTS', VAR_BATCH_ID, NULL, NULL, VAR_BATCH_GROUPING_ID, FALSE);

INSERT INTO EXECUTION_KEY_VALUE_METADATA ("GROUP", "KEY", VALUE)
	VALUES('SERVICE_FILE_NAMES', 'EVT_DIP_TO_BPE_FOR_SAS_RESULTS', 'DIP_TO_BODS_ASSISTANCE_ENTRIES_SERVICE_' || chr(36) || '{TIMESTAMP_HHMMSS}.json');
	

EXCEPTION WHEN OTHERS THEN 
	L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD;
 
END; 

END_PROC;