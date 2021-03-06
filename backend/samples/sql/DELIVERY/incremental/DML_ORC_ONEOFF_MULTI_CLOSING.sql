INSERT INTO ORCHESTRATION_PROCESS_EXECUTION_CLOSING (ORCHESTRATION_PROCESS_EXECUTION_ID, CLOSING_TYPE_CODE, CLOSING_DATE, CLOSING_PERIOD) 
	SELECT ORCHESTRATION_PROCESS_EXECUTION_ID, CLOSING_TYPE_CODE, CLOSING_DATE, CLOSING_PERIOD FROM ORCHESTRATION_PROCESS_EXECUTION;
INSERT INTO BATCH_INSTANCE_CLOSING (BATCH_INSTANCE_ID, CLOSING_TYPE_CODE, CLOSING_DATE, CLOSING_PERIOD) 
	SELECT BATCH_INSTANCE_ID, CLOSING_TYPE_CODE, CLOSING_DATE, CLOSING_PERIOD FROM BATCH_INSTANCE;
COMMIT;

DROP TABLE ORCHESTRATION_PROCESS_EXECUTION2 IF EXISTS;
CREATE TABLE ORCHESTRATION_PROCESS_EXECUTION2 (
	ORCHESTRATION_PROCESS_EXECUTION_ID INTEGER NOT NULL,
	ORCHESTRATION_PROCESS_ID INTEGER NOT NULL,
	EVENT_INSTANCE_ID INTEGER,
	CLIENT_RUN_ID VARCHAR(128),
	SCENARIO_TYPE VARCHAR(32),
	RUN_TYPE VARCHAR(32),
	CREATE_TIME TIMESTAMP NOT NULL,
	START_TIME TIMESTAMP,
	END_TIME TIMESTAMP,
	STATUS VARCHAR(16),
	EXIT_CODE VARCHAR(16),
	EXIT_MESSAGE VARCHAR(10000)
) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
ALTER TABLE ORCHESTRATION_PROCESS_EXECUTION2 ADD CONSTRAINT ORCHESTRATION_PROCESS_EXECUTION2_PK PRIMARY KEY (ORCHESTRATION_PROCESS_EXECUTION_ID);

INSERT INTO ORCHESTRATION_PROCESS_EXECUTION2 (ORCHESTRATION_PROCESS_EXECUTION_ID, ORCHESTRATION_PROCESS_ID, EVENT_INSTANCE_ID, CLIENT_RUN_ID
, SCENARIO_TYPE, RUN_TYPE, CREATE_TIME, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE) SELECT ORCHESTRATION_PROCESS_EXECUTION_ID
, ORCHESTRATION_PROCESS_ID, EVENT_INSTANCE_ID, CLIENT_RUN_ID, SCENARIO_TYPE, RUN_TYPE, CREATE_TIME, START_TIME, END_TIME, STATUS
, EXIT_CODE, EXIT_MESSAGE FROM ORCHESTRATION_PROCESS_EXECUTION;

DROP TABLE ORCHESTRATION_PROCESS_EXECUTION IF EXISTS;
CREATE TABLE ORCHESTRATION_PROCESS_EXECUTION (
	ORCHESTRATION_PROCESS_EXECUTION_ID INTEGER NOT NULL,
	ORCHESTRATION_PROCESS_ID INTEGER NOT NULL,
	EVENT_INSTANCE_ID INTEGER,
	CLIENT_RUN_ID VARCHAR(128),
	SCENARIO_TYPE VARCHAR(32),
	RUN_TYPE VARCHAR(32),
	CREATE_TIME TIMESTAMP NOT NULL,
	START_TIME TIMESTAMP,
	END_TIME TIMESTAMP,
	STATUS VARCHAR(16),
	EXIT_CODE VARCHAR(16),
	EXIT_MESSAGE VARCHAR(10000)
) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
ALTER TABLE ORCHESTRATION_PROCESS_EXECUTION ADD CONSTRAINT ORCHESTRATION_PROCESS_EXECUTION_PK PRIMARY KEY (ORCHESTRATION_PROCESS_EXECUTION_ID);

INSERT INTO ORCHESTRATION_PROCESS_EXECUTION (ORCHESTRATION_PROCESS_EXECUTION_ID, ORCHESTRATION_PROCESS_ID, EVENT_INSTANCE_ID, CLIENT_RUN_ID
, SCENARIO_TYPE, RUN_TYPE, CREATE_TIME, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE) SELECT ORCHESTRATION_PROCESS_EXECUTION_ID
, ORCHESTRATION_PROCESS_ID, EVENT_INSTANCE_ID, CLIENT_RUN_ID, SCENARIO_TYPE, RUN_TYPE, CREATE_TIME, START_TIME, END_TIME, STATUS
, EXIT_CODE, EXIT_MESSAGE FROM ORCHESTRATION_PROCESS_EXECUTION2;

DROP TABLE ORCHESTRATION_PROCESS_EXECUTION2 IF EXISTS;

DROP TABLE BATCH_INSTANCE2 IF EXISTS;
CREATE TABLE BATCH_INSTANCE2 (
	BATCH_INSTANCE_ID INTEGER NOT NULL,
	BATCH_ID INTEGER NOT NULL,
	SCENARIO_TYPE VARCHAR(32),
	RUN_TYPE VARCHAR(32),
	BATCH_KEY VARCHAR(32) NOT NULL
) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
ALTER TABLE BATCH_INSTANCE2 ADD CONSTRAINT BATCH_INSTANCE2_PK PRIMARY KEY (BATCH_INSTANCE_ID);

INSERT INTO BATCH_INSTANCE2 (BATCH_INSTANCE_ID, BATCH_ID, SCENARIO_TYPE, RUN_TYPE, BATCH_KEY) 
SELECT BATCH_INSTANCE_ID, BATCH_ID, SCENARIO_TYPE, RUN_TYPE, BATCH_KEY FROM BATCH_INSTANCE;

DROP TABLE BATCH_INSTANCE IF EXISTS;
CREATE TABLE BATCH_INSTANCE (
	BATCH_INSTANCE_ID INTEGER NOT NULL,
	BATCH_ID INTEGER NOT NULL,
	SCENARIO_TYPE VARCHAR(32),
	RUN_TYPE VARCHAR(32),
	BATCH_KEY VARCHAR(32) NOT NULL
) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
ALTER TABLE BATCH_INSTANCE ADD CONSTRAINT BATCH_INSTANCE_PK PRIMARY KEY (BATCH_INSTANCE_ID);

INSERT INTO BATCH_INSTANCE (BATCH_INSTANCE_ID, BATCH_ID, SCENARIO_TYPE, RUN_TYPE, BATCH_KEY) 
SELECT BATCH_INSTANCE_ID, BATCH_ID, SCENARIO_TYPE, RUN_TYPE, BATCH_KEY FROM BATCH_INSTANCE2;

DROP TABLE BATCH_INSTANCE2 IF EXISTS;

COMMIT;


DROP TABLE EVENT_INSTANCE2 IF EXISTS;
CREATE TABLE EVENT_INSTANCE2
(
	EVENT_INSTANCE_ID	                            INT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
	EVENT_CODE	                                    VARCHAR(128) NOT NULL,
	BUSINESS_CALENDAR_EXECUTION_ID					VARCHAR(1024),
	ORCHESTRATION_PROCESS_EXECUTION_ID 				INT,
	STATUS	                                        VARCHAR(16) NOT NULL,
	CREATE_TIME	                                    TIMESTAMP NOT NULL,
	CREATE_USER	                                    CHAR(8) NOT NULL,
	LAST_UPDATE_TIME	                            TIMESTAMP NOT NULL,
	LAST_UPDATE_USER								CHAR(8) NOT NULL
) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
ALTER TABLE EVENT_INSTANCE2 ADD CONSTRAINT EVENT_INSTANCE2_PK PRIMARY KEY (EVENT_INSTANCE_ID);

INSERT INTO EVENT_INSTANCE2 (EVENT_INSTANCE_ID, EVENT_CODE
, BUSINESS_CALENDAR_EXECUTION_ID, ORCHESTRATION_PROCESS_EXECUTION_ID, STATUS, CREATE_TIME
, CREATE_USER, LAST_UPDATE_TIME, LAST_UPDATE_USER) SELECT EVENT_INSTANCE_ID, EVENT_CODE
, ' ' || BUSINESS_CALENDAR_EXECUTION_ID || ' ', ORCHESTRATION_PROCESS_EXECUTION_ID, STATUS, CREATE_TIME
, CREATE_USER, LAST_UPDATE_TIME, LAST_UPDATE_USER FROM EVENT_INSTANCE;
COMMIT;

DROP TABLE EVENT_INSTANCE IF EXISTS;
CREATE TABLE EVENT_INSTANCE
(
	EVENT_INSTANCE_ID	                            INT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
	EVENT_CODE	                                    VARCHAR(128) NOT NULL,
	BUSINESS_CALENDAR_EXECUTION_ID					VARCHAR(1024),
	ORCHESTRATION_PROCESS_EXECUTION_ID 				INT,
	STATUS	                                        VARCHAR(16) NOT NULL,
	CREATE_TIME	                                    TIMESTAMP NOT NULL,
	CREATE_USER	                                    CHAR(8) NOT NULL,
	LAST_UPDATE_TIME	                            TIMESTAMP NOT NULL,
	LAST_UPDATE_USER								CHAR(8) NOT NULL
) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
ALTER TABLE EVENT_INSTANCE ADD CONSTRAINT EVENT_INSTANCE_PK PRIMARY KEY (EVENT_INSTANCE_ID);

INSERT INTO EVENT_INSTANCE (EVENT_INSTANCE_ID, EVENT_CODE
, BUSINESS_CALENDAR_EXECUTION_ID, ORCHESTRATION_PROCESS_EXECUTION_ID, STATUS, CREATE_TIME
, CREATE_USER, LAST_UPDATE_TIME, LAST_UPDATE_USER) SELECT EVENT_INSTANCE_ID, EVENT_CODE
, BUSINESS_CALENDAR_EXECUTION_ID, ORCHESTRATION_PROCESS_EXECUTION_ID, STATUS, CREATE_TIME
, CREATE_USER, LAST_UPDATE_TIME, LAST_UPDATE_USER FROM EVENT_INSTANCE2;

DROP TABLE EVENT_INSTANCE2 IF EXISTS;

COMMIT;
