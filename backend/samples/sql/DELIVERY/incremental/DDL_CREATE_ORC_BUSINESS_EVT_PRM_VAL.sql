DROP TABLE BUSINESS_EVENT_PARAM_VALUATION IF EXISTS;
CREATE TABLE BUSINESS_EVENT_PARAM_VALUATION
(
	BUSINESS_EVENT_PARAM_VALUATION_ID    INT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
	BUSINESS_EVENT_ID                    INT NOT NULL,
	DATA_SET							 VARCHAR(32),
	PARAMETER_NAMES	                     VARCHAR(1024) NOT NULL,
	PARENT_ORCHESTRATION_PROCESS_IDS	 VARCHAR(256),
	PARENT_BATCH_IDS                     VARCHAR(256),
	PARENT_CLOSING_TYPE_CODE			 VARCHAR(32),
	PARENT_SCENARIO_TYPE			 	 VARCHAR(32),
	PARENT_RUN_TYPE			 			 VARCHAR(32),
	PARENT_CLOSING_PERIOD	             VARCHAR(32),
	DEFAULTING_RULE	                     VARCHAR(1024),
	VALUATION_PROCESSING                 VARCHAR(256),
	SPECIFIC_PROCESS	                 VARCHAR(2500)
 ) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
ALTER TABLE BUSINESS_EVENT_PARAM_VALUATION ADD CONSTRAINT BUSINESS_EVENT_PARAM_VALUATION_PK PRIMARY KEY (BUSINESS_EVENT_PARAM_VALUATION_ID);