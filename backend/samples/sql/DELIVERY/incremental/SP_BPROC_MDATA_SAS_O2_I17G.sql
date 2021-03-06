CREATE OR REPLACE PROCEDURE SP_BUSINESS_PROCESS_MDATA_SAS_OMEGA_I17G () RETURNS INTEGER
   LANGUAGE nzplsql AS BEGIN_PROC

DECLARE VAR_BUSINESS_PROCESS_ID_01 INT ;
VAR_BUSINESS_PROCESS_STEP_ID_01 INT ;
VAR_BUSINESS_EVENT_ID_01 INT ;
VAR_BUSINESS_EVENT_ID_02 INT ;

L_ERR_CD				CHAR(5);
L_ERR_MSG				VARCHAR(32000);


BEGIN
SET ISOLATION TO UR;

SET VAR_BUSINESS_PROCESS_ID_01 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;

INSERT INTO BUSINESS_PROCESS (BUSINESS_PROCESS_ID, BUSINESS_PROCESS_NAME, CLOSING_TYPE_CODE, SCENARIO_TYPE, RUN_TYPE)
	VALUES(VAR_BUSINESS_PROCESS_ID_01, 'Business Process for DIP to SAS to DIP and to Omega and BPE Data Flow', 'I17G', 'CLOSING', 'POSTING');


SET VAR_BUSINESS_PROCESS_STEP_ID_01 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;
SET VAR_BUSINESS_EVENT_ID_01 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;
SET VAR_BUSINESS_EVENT_ID_02 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;

INSERT INTO BUSINESS_PROCESS_STEP (BUSINESS_PROCESS_STEP_ID, BUSINESS_PROCESS_STEP_NAME, BUSINESS_PROCESS_ID, STEP_SEQUENCE)
	VALUES(VAR_BUSINESS_PROCESS_STEP_ID_01, 'DIP to SAS to DIP Data Flow', VAR_BUSINESS_PROCESS_ID_01, 1);

INSERT INTO BUSINESS_EVENT (BUSINESS_EVENT_ID, EVENT_CODE, CLOSING_TYPE_CODE, SCENARIO_TYPE, RUN_TYPE, IS_ACTIVE, TRIGGER_MODE, DELAY_DAYS)
	VALUES(VAR_BUSINESS_EVENT_ID_01, 'EVT_DIP_TO_SAS_DATA', 'I17G', 'CLOSING', 'POSTING', true, 'DIP Triggered', NULL);

INSERT INTO BUSINESS_EVENT (BUSINESS_EVENT_ID, EVENT_CODE, CLOSING_TYPE_CODE, SCENARIO_TYPE, RUN_TYPE, IS_ACTIVE, TRIGGER_MODE, DELAY_DAYS)
	VALUES(VAR_BUSINESS_EVENT_ID_02, 'EVT_SAS_TO_DIP_RESULTS', 'I17G', 'CLOSING', 'POSTING', true, 'External System Triggered', NULL);

INSERT INTO BUSINESS_PROCESS_STEP_EVENT_MAPPING (BUSINESS_PROCESS_STEP_ID, BUSINESS_EVENT_ID)
	VALUES(VAR_BUSINESS_PROCESS_STEP_ID_01, VAR_BUSINESS_EVENT_ID_01);


SET VAR_BUSINESS_PROCESS_STEP_ID_01 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;
SET VAR_BUSINESS_EVENT_ID_01 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;

INSERT INTO BUSINESS_PROCESS_STEP (BUSINESS_PROCESS_STEP_ID, BUSINESS_PROCESS_STEP_NAME, BUSINESS_PROCESS_ID, STEP_SEQUENCE)
	VALUES(VAR_BUSINESS_PROCESS_STEP_ID_01, 'DIP to Omega (SAS Results) Data Flow', VAR_BUSINESS_PROCESS_ID_01, 2);

INSERT INTO BUSINESS_EVENT (BUSINESS_EVENT_ID, EVENT_CODE, CLOSING_TYPE_CODE, SCENARIO_TYPE, RUN_TYPE, IS_ACTIVE, TRIGGER_MODE, DELAY_DAYS)
	VALUES(VAR_BUSINESS_EVENT_ID_01, 'EVT_DIP_TO_OMEGA_RUN_FOR_SAS_RESULTS', 'I17G', 'CLOSING', 'POSTING', true, 'DIP Triggered', NULL);

INSERT INTO BUSINESS_PROCESS_STEP_EVENT_MAPPING (BUSINESS_PROCESS_STEP_ID, BUSINESS_EVENT_ID)
	VALUES(VAR_BUSINESS_PROCESS_STEP_ID_01, VAR_BUSINESS_EVENT_ID_01);


SET VAR_BUSINESS_PROCESS_STEP_ID_01 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;
SET VAR_BUSINESS_EVENT_ID_01 = NEXT VALUE FOR SEQUENCE_BUSINESS_CALENDAR_METADATA;

INSERT INTO BUSINESS_PROCESS_STEP (BUSINESS_PROCESS_STEP_ID, BUSINESS_PROCESS_STEP_NAME, BUSINESS_PROCESS_ID, STEP_SEQUENCE)
	VALUES(VAR_BUSINESS_PROCESS_STEP_ID_01, 'DIP to BPE (SAS Results) Data Flow', VAR_BUSINESS_PROCESS_ID_01, 3);

INSERT INTO BUSINESS_EVENT (BUSINESS_EVENT_ID, EVENT_CODE, CLOSING_TYPE_CODE, SCENARIO_TYPE, RUN_TYPE, IS_ACTIVE, TRIGGER_MODE, DELAY_DAYS)
	VALUES(VAR_BUSINESS_EVENT_ID_01, 'EVT_DIP_TO_BPE_FOR_SAS_RESULTS', 'I17G', 'CLOSING', 'POSTING', true, 'External System Triggered', NULL);

INSERT INTO BUSINESS_PROCESS_STEP_EVENT_MAPPING (BUSINESS_PROCESS_STEP_ID, BUSINESS_EVENT_ID)
	VALUES(VAR_BUSINESS_PROCESS_STEP_ID_01, VAR_BUSINESS_EVENT_ID_01);


EXCEPTION WHEN OTHERS THEN 
	L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD; 
 
END; 

END_PROC;