CREATE OR REPLACE PROCEDURE SP_BATCH_METADATA_FOR_PNC_EXP () RETURNS INTEGER
   LANGUAGE nzplsql AS BEGIN_PROC

DECLARE 
VAR_BATCH_GROUPING_ID INT;
VAR_BATCH_ID INT;
VAR_JOB_GROUP_ID INT;
VAR_JOB_ID INT;
VAR_STEP_01 INT;
VAR_STEP_02 INT;
L_ERR_CD				CHAR(5);
L_ERR_MSG				VARCHAR(32000);

BEGIN
SET ISOLATION TO UR;

SET VAR_BATCH_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_GROUP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_ID  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP_01  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP_02  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO BATCH (BATCH_ID,BATCH_NAME,BATCH_DESCRIPTION,APPLICATION_NAME,BATCH_PRIORITY,ON_ERROR,PRE_BATCH_PROCESSING,POST_BATCH_PROCESSING) VALUES
(VAR_BATCH_ID, 'DIP to Omega Run for Discount', 'DIP to Omega Run for Discount', 'DIP', 1, NULL, NULL, NULL);

INSERT INTO JOB_GROUP (JOB_GROUP_ID,JOB_GROUP_NAME,BATCH_ID,PARENT_JOB_GROUP_ID) VALUES
(VAR_JOB_GROUP_ID,'Extract Discount Data for Omega Run Job Group',VAR_BATCH_ID,NULL);

INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN, JOB_PARAMS)
        VALUES ( VAR_JOB_ID, 'Extract Discount Data for Omega', NULL, VAR_JOB_GROUP_ID, NULL, NULL, NULL);
		
INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_01, 4, VAR_JOB_ID, 'Discount - Data extraction', 1, NULL, NULL, 'binary-dip:dbsql-query-explicit', NULL);
		
INSERT INTO DATA_EXPOSITION_STEP (STEP_ID, SCHEMA_NAME, TABLE_NAME, DELIMITER, INCLUDE_HEADER, SPECIFIC_PROCESS, FILE_DIRECTORY, FILE_NAME_PATTERN, DATA_FILE_TREATMENT)
		VALUES(VAR_STEP_01, NULL, NULL, '	', true, 'SELECT SUBSIDIARY AS FILIALE, SUBLEDGER, SEGMENT, LOB, CURRENCY_CODE AS DEVISE, CODE AS NORME, TYPE, Y1 AS "1", Y2 AS "2", Y3 AS "3", Y4 AS "4", Y5 AS "5", Y6 AS "6", Y7 AS "7", Y8 AS "8", Y9 AS "9", Y10 AS "10", Y11 AS "11", Y12 AS "12", Y13 AS "13", Y14 AS "14", Y15 AS "15", Y16 AS "16", Y17 AS "17", Y18 AS "18", Y19 AS "19", Y20 AS "20", Y21 AS "21", Y22 AS "22", Y23 AS "23", Y24 AS "24", Y25 AS "25", Y26 AS "26", Y27 AS "27", Y28 AS "28", Y29 AS "29", Y30 AS "30", Y31 AS "31", Y32 AS "32", Y33 AS "33", Y34 AS "34", Y35 AS "35", Y36 AS "36", Y37 AS "37", Y38 AS "38", Y39 AS "39", Y40 AS "40", Y41 AS "41", Y42 AS "42", Y43 AS "43", Y44 AS "44", Y45 AS "45", Y46 AS "46", Y47 AS "47", Y48 AS "48", Y49 AS "49", Y50 AS "50", Y51 AS "51", Y52 AS "52", Y53 AS "53", Y54 AS "54", Y55 AS "55", Y56 AS "56", Y57 AS "57", Y58 AS "58", Y59 AS "59", Y60 AS "60", Y61 AS "61", Y62 AS "62", Y63 AS "63", Y64 AS "64", Y65 AS "65" FROM ' || chr(36) || '{DWH_DB}.V_OMEGA_DISCOUNT a WHERE a.CLOSING_DATE = ''' || chr(36) || '{CLOSING_DATE}'' AND a.CODE = ''' || chr(36) || '{CLOSING_TYPE}''', 'omega/out'
		, 'DIP_DSC_' || chr(36) || '{CLOSING_TYPE}_' || chr(36) || '{CLOSING_DATE_YYYYMMDD}_' || chr(36) || '{TIMESTAMPHHMMSS}.dat', NULL);

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_02, 4, VAR_JOB_ID, 'Discount - SFTP file to Omega', 2, NULL, NULL, 'sftp:omegapnc-to', NULL);
		
INSERT INTO DATA_EXPOSITION_STEP (STEP_ID, SCHEMA_NAME, TABLE_NAME, DELIMITER, INCLUDE_HEADER, SPECIFIC_PROCESS, FILE_DIRECTORY, FILE_NAME_PATTERN, DATA_FILE_TREATMENT)
		VALUES(VAR_STEP_02, NULL, NULL, NULL, NULL, NULL, 'omega/out'
		, 'DIP_DSC_' || chr(36) || '{CLOSING_TYPE}_' || chr(36) || '{CLOSING_DATE_YYYYMMDD}_' || chr(36) || '{TIMESTAMPHHMMSS}.dat', 'ARCHIVE');


SELECT BATCH_GROUPING_ID INTO VAR_BATCH_GROUPING_ID FROM BATCH_GROUPING WHERE BATCH_GROUPING_NAME = 'YIELD CURVE PROCESSING';

IF VAR_BATCH_GROUPING_ID IS NULL THEN 
	SET VAR_BATCH_GROUPING_ID = NEXT VALUE FOR SEQUENCE_BATCH_GROUPING;

	INSERT INTO BATCH_GROUPING (BATCH_GROUPING_ID, BATCH_GROUPING_NAME, ACTIVE_DWH, PASSIVE_DWH, LAST_UPDATE_TIME)
		VALUES(VAR_BATCH_GROUPING_ID, 'YIELD CURVE PROCESSING', 'DWHD2_' || chr(36) || '{ENV}', 'DWHD1_' || chr(36) || '{ENV}', CURRENT_TIMESTAMP);

END IF;

INSERT INTO EVENT ( EVENT_CODE, EVENT_DESCRIPTION )
        VALUES ( 'EVT_DIP_TO_OMEGA_FOR_DSC', 'Event to Process DIP to Omega for Discount' );

INSERT INTO EVENT_MAPPING ( EVENT_CODE, BATCH_ID, ORCHESTRATION_PROCESS_STEP_ID, ORCHESTRATION_PROCESS_ID, BATCH_GROUPING_ID, IS_DWH_SWITCH)
        VALUES ( 'EVT_DIP_TO_OMEGA_FOR_DSC', VAR_BATCH_ID, NULL, NULL, VAR_BATCH_GROUPING_ID, FALSE);



SET VAR_BATCH_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_GROUP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_ID  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP_01  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP_02  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO BATCH (BATCH_ID,BATCH_NAME,BATCH_DESCRIPTION,APPLICATION_NAME,BATCH_PRIORITY,ON_ERROR,PRE_BATCH_PROCESSING,POST_BATCH_PROCESSING) VALUES
(VAR_BATCH_ID, 'DIP to Omega Run for LKR', 'DIP to Omega Run for LKR', 'DIP', 1, NULL, NULL, NULL);

INSERT INTO JOB_GROUP (JOB_GROUP_ID,JOB_GROUP_NAME,BATCH_ID,PARENT_JOB_GROUP_ID) VALUES
(VAR_JOB_GROUP_ID,'Extract LKR Data for Omega Run Job Group',VAR_BATCH_ID,NULL);

INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN, JOB_PARAMS)
        VALUES ( VAR_JOB_ID, 'Extract LKR Data for Omega', NULL, VAR_JOB_GROUP_ID, NULL, NULL, NULL);

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_01, 4, VAR_JOB_ID, 'LKR - Data extraction', 1, NULL, NULL, 'binary-dip:dbsql-query-explicit', NULL);

INSERT INTO DATA_EXPOSITION_STEP (STEP_ID, SCHEMA_NAME, TABLE_NAME, DELIMITER, INCLUDE_HEADER, SPECIFIC_PROCESS, FILE_DIRECTORY, FILE_NAME_PATTERN, DATA_FILE_TREATMENT)
		VALUES(VAR_STEP_01, NULL, NULL, '	', true, 'SELECT SUBSIDIARY AS FILIALE, SUBLEDGER, SEGMENT, LOB, CURRENCY_CODE AS DEVISE, CODE AS NORME, TYPE, Y1 AS "1", Y2 AS "2", Y3 AS "3", Y4 AS "4", Y5 AS "5", Y6 AS "6", Y7 AS "7", Y8 AS "8", Y9 AS "9", Y10 AS "10", Y11 AS "11", Y12 AS "12", Y13 AS "13", Y14 AS "14", Y15 AS "15", Y16 AS "16", Y17 AS "17", Y18 AS "18", Y19 AS "19", Y20 AS "20", Y21 AS "21", Y22 AS "22", Y23 AS "23", Y24 AS "24", Y25 AS "25", Y26 AS "26", Y27 AS "27", Y28 AS "28", Y29 AS "29", Y30 AS "30", Y31 AS "31", Y32 AS "32", Y33 AS "33", Y34 AS "34", Y35 AS "35", Y36 AS "36", Y37 AS "37", Y38 AS "38", Y39 AS "39", Y40 AS "40", Y41 AS "41", Y42 AS "42", Y43 AS "43", Y44 AS "44", Y45 AS "45", Y46 AS "46", Y47 AS "47", Y48 AS "48", Y49 AS "49", Y50 AS "50", Y51 AS "51", Y52 AS "52", Y53 AS "53", Y54 AS "54", Y55 AS "55", Y56 AS "56", Y57 AS "57", Y58 AS "58", Y59 AS "59", Y60 AS "60", Y61 AS "61", Y62 AS "62", Y63 AS "63", Y64 AS "64", Y65 AS "65" FROM ' || chr(36) || '{DWH_DB}.V_OMEGA_LKR a WHERE a.CLOSING_DATE = ''' || chr(36) || '{CLOSING_DATE}'' AND a.CODE = ''' || chr(36) || '{CLOSING_TYPE}''', 'omega/out'
		, 'DIP_LKR_' || chr(36) || '{CLOSING_TYPE}_' || chr(36) || '{CLOSING_DATE_YYYYMMDD}_' || chr(36) || '{TIMESTAMPHHMMSS}.dat', NULL);

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_02, 4, VAR_JOB_ID, 'LKR - SFTP file to Omega', 2, NULL, NULL, 'sftp:omegapnc-to', NULL);
		
INSERT INTO DATA_EXPOSITION_STEP (STEP_ID, SCHEMA_NAME, TABLE_NAME, DELIMITER, INCLUDE_HEADER, SPECIFIC_PROCESS, FILE_DIRECTORY, FILE_NAME_PATTERN, DATA_FILE_TREATMENT)
		VALUES(VAR_STEP_02, NULL, NULL, NULL, NULL, NULL, 'omega/out'
		, 'DIP_LKR_' || chr(36) || '{CLOSING_TYPE}_' || chr(36) || '{CLOSING_DATE_YYYYMMDD}_' || chr(36) || '{TIMESTAMPHHMMSS}.dat', 'ARCHIVE');


INSERT INTO EVENT ( EVENT_CODE, EVENT_DESCRIPTION )
        VALUES ( 'EVT_DIP_TO_OMEGA_FOR_LKR', 'Event to Process DIP to Omega for LKR' );

INSERT INTO EVENT_MAPPING ( EVENT_CODE, BATCH_ID, ORCHESTRATION_PROCESS_STEP_ID, ORCHESTRATION_PROCESS_ID, BATCH_GROUPING_ID, IS_DWH_SWITCH)
        VALUES ( 'EVT_DIP_TO_OMEGA_FOR_LKR', VAR_BATCH_ID, NULL, NULL, VAR_BATCH_GROUPING_ID, FALSE);


SET VAR_BATCH_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_GROUP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_ID  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP_01  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP_02  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO BATCH (BATCH_ID,BATCH_NAME,BATCH_DESCRIPTION,APPLICATION_NAME,BATCH_PRIORITY,ON_ERROR,PRE_BATCH_PROCESSING,POST_BATCH_PROCESSING) VALUES
(VAR_BATCH_ID, 'DIP to Omega Run for FWD', 'DIP to Omega Run for FWD', 'DIP', 1, NULL, NULL, NULL);

INSERT INTO JOB_GROUP (JOB_GROUP_ID,JOB_GROUP_NAME,BATCH_ID,PARENT_JOB_GROUP_ID) VALUES
(VAR_JOB_GROUP_ID,'Extract FWD Data for Omega Run Job Group',VAR_BATCH_ID,NULL);

INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN, JOB_PARAMS)
        VALUES ( VAR_JOB_ID, 'Extract FWD Data for Omega', NULL, VAR_JOB_GROUP_ID, NULL, NULL, NULL);

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE, IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_01, 4, VAR_JOB_ID, 'FWD - Data extraction', 1, NULL, NULL, 'binary-dip:dbsql-query-explicit', NULL);
		
INSERT INTO DATA_EXPOSITION_STEP (STEP_ID, SCHEMA_NAME, TABLE_NAME, DELIMITER, INCLUDE_HEADER, SPECIFIC_PROCESS, FILE_DIRECTORY, FILE_NAME_PATTERN, DATA_FILE_TREATMENT)
		VALUES(VAR_STEP_01, NULL, NULL, '	', true, 'SELECT SUBSIDIARY AS FILIALE, SUBLEDGER, SEGMENT, LOB, CURRENCY_CODE AS DEVISE, CODE AS NORME, TYPE, Y1 AS "1", Y2 AS "2", Y3 AS "3", Y4 AS "4", Y5 AS "5", Y6 AS "6", Y7 AS "7", Y8 AS "8", Y9 AS "9", Y10 AS "10", Y11 AS "11", Y12 AS "12", Y13 AS "13", Y14 AS "14", Y15 AS "15", Y16 AS "16", Y17 AS "17", Y18 AS "18", Y19 AS "19", Y20 AS "20", Y21 AS "21", Y22 AS "22", Y23 AS "23", Y24 AS "24", Y25 AS "25", Y26 AS "26", Y27 AS "27", Y28 AS "28", Y29 AS "29", Y30 AS "30", Y31 AS "31", Y32 AS "32", Y33 AS "33", Y34 AS "34", Y35 AS "35", Y36 AS "36", Y37 AS "37", Y38 AS "38", Y39 AS "39", Y40 AS "40", Y41 AS "41", Y42 AS "42", Y43 AS "43", Y44 AS "44", Y45 AS "45", Y46 AS "46", Y47 AS "47", Y48 AS "48", Y49 AS "49", Y50 AS "50", Y51 AS "51", Y52 AS "52", Y53 AS "53", Y54 AS "54", Y55 AS "55", Y56 AS "56", Y57 AS "57", Y58 AS "58", Y59 AS "59", Y60 AS "60", Y61 AS "61", Y62 AS "62", Y63 AS "63", Y64 AS "64", Y65 AS "65" FROM ' || chr(36) || '{DWH_DB}.V_OMEGA_FORWARD a WHERE a.CLOSING_DATE = ''' || chr(36) || '{CLOSING_DATE}'' AND a.CODE = ''' || chr(36) || '{CLOSING_TYPE}''', 'omega/out'
		, 'DIP_FWD_' || chr(36) || '{CLOSING_TYPE}_' || chr(36) || '{CLOSING_DATE_YYYYMMDD}_' || chr(36) || '{TIMESTAMPHHMMSS}.dat', NULL);

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE, IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_02, 4, VAR_JOB_ID, 'FWD - SFTP file to Omega', 2, NULL, NULL, 'sftp:omegapnc-to', NULL);

INSERT INTO DATA_EXPOSITION_STEP (STEP_ID, SCHEMA_NAME, TABLE_NAME, DELIMITER, INCLUDE_HEADER, SPECIFIC_PROCESS, FILE_DIRECTORY, FILE_NAME_PATTERN, DATA_FILE_TREATMENT)
		VALUES(VAR_STEP_02, NULL, NULL, NULL, NULL, NULL, 'omega/out'
		, 'DIP_FWD_' || chr(36) || '{CLOSING_TYPE}_' || chr(36) || '{CLOSING_DATE_YYYYMMDD}_' || chr(36) || '{TIMESTAMPHHMMSS}.dat', 'ARCHIVE');


INSERT INTO EVENT ( EVENT_CODE, EVENT_DESCRIPTION )
        VALUES ( 'EVT_DIP_TO_OMEGA_FOR_FWD', 'Event to Process DIP to Omega for Forward' );

INSERT INTO EVENT_MAPPING ( EVENT_CODE, BATCH_ID, ORCHESTRATION_PROCESS_STEP_ID, ORCHESTRATION_PROCESS_ID, BATCH_GROUPING_ID, IS_DWH_SWITCH)
        VALUES ( 'EVT_DIP_TO_OMEGA_FOR_FWD', VAR_BATCH_ID, NULL, NULL, VAR_BATCH_GROUPING_ID, FALSE);


SET VAR_BATCH_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_GROUP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_ID  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP_01  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP_02  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO BATCH (BATCH_ID,BATCH_NAME,BATCH_DESCRIPTION,APPLICATION_NAME,BATCH_PRIORITY,ON_ERROR,PRE_BATCH_PROCESSING,POST_BATCH_PROCESSING) VALUES
(VAR_BATCH_ID, 'DIP to Omega Run for RA', 'DIP to Omega Run for RA', 'DIP', 1, NULL, NULL, NULL);

INSERT INTO JOB_GROUP (JOB_GROUP_ID,JOB_GROUP_NAME,BATCH_ID,PARENT_JOB_GROUP_ID) VALUES
(VAR_JOB_GROUP_ID,'Extract RA Data for Omega Run Job Group',VAR_BATCH_ID,NULL);

INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN, JOB_PARAMS)
        VALUES ( VAR_JOB_ID, 'Extract RA Data for Omega', NULL, VAR_JOB_GROUP_ID, NULL, NULL, NULL);

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_01, 4, VAR_JOB_ID, 'RA Data - Data extraction', 1, NULL, NULL, 'binary-dip:dbsql-query-explicit', NULL);

INSERT INTO DATA_EXPOSITION_STEP (STEP_ID, SCHEMA_NAME, TABLE_NAME, DELIMITER, INCLUDE_HEADER, SPECIFIC_PROCESS, FILE_DIRECTORY, FILE_NAME_PATTERN, DATA_FILE_TREATMENT)
		VALUES(VAR_STEP_01, NULL, NULL, '	', true, 'SELECT SUBSIDIARY, SUBLEDGER, LOB_N1, NATURE, NORM, DOMAIN, PREMIUM, RESERVES FROM ' || chr(36) || '{DWH_DB}.V_OMEGA_RA a WHERE a.CLOSING_DATE = ''' || chr(36) || '{CLOSING_DATE}'' AND a.NORM = ''' || chr(36) || '{CLOSING_TYPE}''', 'omega/out'
		, 'DIP_RA_' || chr(36) || '{CLOSING_TYPE}_' || chr(36) || '{CLOSING_DATE_YYYYMMDD}_' || chr(36) || '{TIMESTAMPHHMMSS}.dat', NULL);
		
INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_02, 4, VAR_JOB_ID, 'RA Data - SFTP file to Omega', 2, NULL, NULL, 'sftp:omegapnc-to', NULL);
		
INSERT INTO DATA_EXPOSITION_STEP (STEP_ID, SCHEMA_NAME, TABLE_NAME, DELIMITER, INCLUDE_HEADER, SPECIFIC_PROCESS, FILE_DIRECTORY, FILE_NAME_PATTERN, DATA_FILE_TREATMENT)
		VALUES(VAR_STEP_02, NULL, NULL, NULL, NULL, NULL, 'omega/out'
		, 'DIP_RA_' || chr(36) || '{CLOSING_TYPE}_' || chr(36) || '{CLOSING_DATE_YYYYMMDD}_' || chr(36) || '{TIMESTAMPHHMMSS}.dat', 'ARCHIVE');
		

SELECT BATCH_GROUPING_ID INTO VAR_BATCH_GROUPING_ID FROM BATCH_GROUPING WHERE BATCH_GROUPING_NAME = 'P&C RISK ADJUSTMENT';

IF VAR_BATCH_GROUPING_ID IS NULL THEN 
	SET VAR_BATCH_GROUPING_ID = NEXT VALUE FOR SEQUENCE_BATCH_GROUPING;

	INSERT INTO BATCH_GROUPING (BATCH_GROUPING_ID, BATCH_GROUPING_NAME, ACTIVE_DWH, PASSIVE_DWH, LAST_UPDATE_TIME)
		VALUES(VAR_BATCH_GROUPING_ID, 'P&C RISK ADJUSTMENT', 'DWHD2_' || chr(36) || '{ENV}', 'DWHD1_' || chr(36) || '{ENV}', CURRENT_TIMESTAMP);

END IF;

INSERT INTO EVENT ( EVENT_CODE, EVENT_DESCRIPTION )
        VALUES ( 'EVT_DIP_TO_OMEGA_FOR_RA', 'Event to Process DIP to Omega for Risk Adjustment' );

INSERT INTO EVENT_MAPPING ( EVENT_CODE, BATCH_ID, ORCHESTRATION_PROCESS_STEP_ID, ORCHESTRATION_PROCESS_ID, BATCH_GROUPING_ID, IS_DWH_SWITCH)
        VALUES ( 'EVT_DIP_TO_OMEGA_FOR_RA', VAR_BATCH_ID, NULL, NULL, VAR_BATCH_GROUPING_ID, FALSE);


SET VAR_BATCH_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_GROUP_ID = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_JOB_ID  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP_01  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;
SET VAR_STEP_02  = NEXT VALUE FOR SEQUENCE_BATCH_METADATA;

INSERT INTO BATCH (BATCH_ID,BATCH_NAME,BATCH_DESCRIPTION,APPLICATION_NAME,BATCH_PRIORITY,ON_ERROR,PRE_BATCH_PROCESSING,POST_BATCH_PROCESSING) VALUES
(VAR_BATCH_ID, 'DIP to Omega Run for Expense Ratio', 'DIP to Omega Run for Expense Ratio', 'DIP', 1, NULL, NULL, NULL);

INSERT INTO JOB_GROUP (JOB_GROUP_ID,JOB_GROUP_NAME,BATCH_ID,PARENT_JOB_GROUP_ID) VALUES
(VAR_JOB_GROUP_ID,'Extract Expense Ratio Data for Omega Run Job Group',VAR_BATCH_ID,NULL);

INSERT INTO JOB ( JOB_ID, JOB_NAME, JOB_PRIORITY, JOB_GROUP_ID, IS_ACTIVE, JOB_OPTIONAL_RUN, JOB_PARAMS)
        VALUES ( VAR_JOB_ID, 'Extract Expense Ratio Data for Omega', NULL, VAR_JOB_GROUP_ID, NULL, NULL, NULL);

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_01, 4, VAR_JOB_ID, 'Expense Ratio - Data extraction', 1, NULL, NULL, 'binary-dip:dbsql-query-explicit', NULL);
		
INSERT INTO DATA_EXPOSITION_STEP (STEP_ID, SCHEMA_NAME, TABLE_NAME, DELIMITER, INCLUDE_HEADER, SPECIFIC_PROCESS, FILE_DIRECTORY, FILE_NAME_PATTERN, DATA_FILE_TREATMENT)
		VALUES(VAR_STEP_01, NULL, NULL, '	', true, 'SELECT SUBSIDIARY, SUBLEDGER, MARKET, NATURE, NORM, ACQUISITION, MAINTENANCE FROM ' || chr(36) || '{DWH_DB}.V_OMEGA_RATIO a WHERE a.CLOSING_DATE = ''' || chr(36) || '{CLOSING_DATE}'' AND a.NORM = ''' || chr(36) || '{CLOSING_TYPE}''', 'omega/out'
		, 'DIP_RATIO_' || chr(36) || '{CLOSING_TYPE}_' || chr(36) || '{CLOSING_DATE_YYYYMMDD}_' || chr(36) || '{TIMESTAMPHHMMSS}.dat', NULL);

INSERT INTO STEP ( STEP_ID, STEP_TYPE_ID, JOB_ID, STEP_NAME, STEP_SEQUENCE ,IS_ACTIVE, PRE_STEP_PROCESSING,STEP_PROCESSING, POST_STEP_PROCESSING)
        VALUES ( VAR_STEP_02, 4, VAR_JOB_ID, 'RATIO - SFTP file to Omega', 2, NULL, NULL, 'sftp:omegapnc-to', NULL);
		
INSERT INTO DATA_EXPOSITION_STEP (STEP_ID, SCHEMA_NAME, TABLE_NAME, DELIMITER, INCLUDE_HEADER, SPECIFIC_PROCESS, FILE_DIRECTORY, FILE_NAME_PATTERN, DATA_FILE_TREATMENT)
		VALUES(VAR_STEP_02, NULL, NULL, NULL, NULL, NULL, 'omega/out'
		, 'DIP_RATIO_' || chr(36) || '{CLOSING_TYPE}_' || chr(36) || '{CLOSING_DATE_YYYYMMDD}_' || chr(36) || '{TIMESTAMPHHMMSS}.dat', 'ARCHIVE');


SELECT BATCH_GROUPING_ID INTO VAR_BATCH_GROUPING_ID FROM BATCH_GROUPING WHERE BATCH_GROUPING_NAME = 'P&C EXPENSE RATIO';

IF VAR_BATCH_GROUPING_ID IS NULL THEN 
	SET VAR_BATCH_GROUPING_ID = NEXT VALUE FOR SEQUENCE_BATCH_GROUPING;

	INSERT INTO BATCH_GROUPING (BATCH_GROUPING_ID, BATCH_GROUPING_NAME, ACTIVE_DWH, PASSIVE_DWH, LAST_UPDATE_TIME)
		VALUES(VAR_BATCH_GROUPING_ID, 'P&C EXPENSE RATIO', 'DWHD2_' || chr(36) || '{ENV}', 'DWHD1_' || chr(36) || '{ENV}', CURRENT_TIMESTAMP);

END IF;

INSERT INTO EVENT ( EVENT_CODE, EVENT_DESCRIPTION )
        VALUES ( 'EVT_DIP_TO_OMEGA_FOR_RATIO', 'Event to Process DIP to Omega for Expense Ratio' );

INSERT INTO EVENT_MAPPING ( EVENT_CODE, BATCH_ID, ORCHESTRATION_PROCESS_STEP_ID, ORCHESTRATION_PROCESS_ID, BATCH_GROUPING_ID, IS_DWH_SWITCH)
        VALUES ( 'EVT_DIP_TO_OMEGA_FOR_RATIO', VAR_BATCH_ID, NULL, NULL, VAR_BATCH_GROUPING_ID, FALSE);


		
EXCEPTION WHEN OTHERS THEN 
	L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD; 
 
END; 

END_PROC;