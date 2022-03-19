CREATE OR REPLACE PROCEDURE SP_BATCH_GROUPING () RETURNS INTEGER
   LANGUAGE nzplsql AS BEGIN_PROC

DECLARE VAR_BATCH_GROUPING_ID INT;
L_ERR_CD				CHAR(5);
L_ERR_MSG				VARCHAR(32000);


BEGIN
SET ISOLATION TO UR;

SET VAR_BATCH_GROUPING_ID = NEXT VALUE FOR SEQUENCE_BATCH_GROUPING;

DELETE FROM BATCH_GROUPING WHERE BATCH_GROUPING_NAME = 'LIFE MODELING PROCESSING';
INSERT INTO BATCH_GROUPING (BATCH_GROUPING_ID, BATCH_GROUPING_NAME, ACTIVE_DWH, PASSIVE_DWH, LAST_UPDATE_TIME)
		VALUES(VAR_BATCH_GROUPING_ID, 'LIFE MODELING PROCESSING', 'DWHD2_' || chr(36) || '{ENV}', 'DWHD1_' || chr(36) || '{ENV}', CURRENT_TIMESTAMP);

SET VAR_BATCH_GROUPING_ID = NEXT VALUE FOR SEQUENCE_BATCH_GROUPING;

DELETE FROM BATCH_GROUPING WHERE BATCH_GROUPING_NAME = 'DIP to SAS PROCESSING';
INSERT INTO BATCH_GROUPING (BATCH_GROUPING_ID, BATCH_GROUPING_NAME, ACTIVE_DWH, PASSIVE_DWH, LAST_UPDATE_TIME)
		VALUES(VAR_BATCH_GROUPING_ID, 'DIP to SAS PROCESSING', 'DWHD2_' || chr(36) || '{ENV}', 'DWHD1_' || chr(36) || '{ENV}', CURRENT_TIMESTAMP);

SET VAR_BATCH_GROUPING_ID = NEXT VALUE FOR SEQUENCE_BATCH_GROUPING;

DELETE FROM BATCH_GROUPING WHERE BATCH_GROUPING_NAME = 'SAS to DIP PROCESSING';
INSERT INTO BATCH_GROUPING (BATCH_GROUPING_ID, BATCH_GROUPING_NAME, ACTIVE_DWH, PASSIVE_DWH, LAST_UPDATE_TIME)
		VALUES(VAR_BATCH_GROUPING_ID, 'SAS to DIP PROCESSING', 'DWHD2_' || chr(36) || '{ENV}', 'DWHD1_' || chr(36) || '{ENV}', CURRENT_TIMESTAMP);

SET VAR_BATCH_GROUPING_ID = NEXT VALUE FOR SEQUENCE_BATCH_GROUPING;

DELETE FROM BATCH_GROUPING WHERE BATCH_GROUPING_NAME = 'LRM PROCESSING';
INSERT INTO BATCH_GROUPING (BATCH_GROUPING_ID, BATCH_GROUPING_NAME, ACTIVE_DWH, PASSIVE_DWH, LAST_UPDATE_TIME)
		VALUES(VAR_BATCH_GROUPING_ID, 'LRM PROCESSING', 'DWHD2_' || chr(36) || '{ENV}', 'DWHD1_' || chr(36) || '{ENV}', CURRENT_TIMESTAMP);

SET VAR_BATCH_GROUPING_ID = NEXT VALUE FOR SEQUENCE_BATCH_GROUPING;

DELETE FROM BATCH_GROUPING WHERE BATCH_GROUPING_NAME = 'FORPLAN PROCESSING';
INSERT INTO BATCH_GROUPING (BATCH_GROUPING_ID, BATCH_GROUPING_NAME, ACTIVE_DWH, PASSIVE_DWH, LAST_UPDATE_TIME)
		VALUES(VAR_BATCH_GROUPING_ID, 'FORPLAN PROCESSING', 'DWHD2_' || chr(36) || '{ENV}', 'DWHD1_' || chr(36) || '{ENV}', CURRENT_TIMESTAMP);

EXCEPTION WHEN OTHERS THEN 
	L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD;
 
END; 

END_PROC;