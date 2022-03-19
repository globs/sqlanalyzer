DROP TABLE BUSINESS_CALENDAR_EXECUTION IF EXISTS;
CREATE TABLE BUSINESS_CALENDAR_EXECUTION
(
	BUSINESS_CALENDAR_EXECUTION_ID   		            INT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
	BUSINESS_CALENDAR_ID                                INT NOT NULL,
	SCENARIO_TYPE			 	    					VARCHAR(32),
	RUN_TYPE											VARCHAR(32),
	TRIGGER_TIME							            TIMESTAMP,
	IS_ACTIVE                                           BOOLEAN,
	TRIGGERING_BATCH_EXECUTION_ID						INT,
	CREATE_TIME                                         TIMESTAMP,
	START_TIME                                          TIMESTAMP,
	END_TIME                                            TIMESTAMP,
	STATUS                                              VARCHAR(16),
	EXIT_CODE                                           VARCHAR(16),
	EXIT_MESSAGE                                        VARCHAR(10000)
	
 ) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
ALTER TABLE BUSINESS_CALENDAR_EXECUTION ADD CONSTRAINT BUSINESS_CALENDAR_EXECUTION_PK PRIMARY KEY (BUSINESS_CALENDAR_EXECUTION_ID);