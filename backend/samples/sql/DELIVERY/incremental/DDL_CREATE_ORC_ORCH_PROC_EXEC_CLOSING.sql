DROP TABLE ORCHESTRATION_PROCESS_EXECUTION_CLOSING IF EXISTS;
CREATE TABLE ORCHESTRATION_PROCESS_EXECUTION_CLOSING
(
	ORCHESTRATION_PROCESS_EXECUTION_ID	    INT NOT NULL,
	CLOSING_TYPE_CODE                   	VARCHAR(32),
	CLOSING_DATE                        	TIMESTAMP,
	CLOSING_PERIOD							VARCHAR(32)
) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
